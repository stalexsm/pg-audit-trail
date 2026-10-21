use serde_json::{Map, Value};

use crate::relation::{Relation, RelationCache, RelationColumn, ReplicaIdentity};
use crate::value::json_from_cell;

pub(crate) type Result<T> = crate::Result<T>;

/// Значение одной колонки кортежа.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cell<'a> {
    /// Текстовое представление значения (DateStyle фиксируется декодированием).
    Text(&'a [u8]),
    /// SQL NULL.
    Null,
    /// Неизменённое TOAST-значение: сервер его не прислал.
    ///
    /// Дочитывание из БД источника — отдельная задача (user story 10 эпика),
    /// в текущем срезе колонка просто не попадает в образ.
    UnchangedToast,
}

/// Тип изменения строки.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

/// Изменение строки: готовые образы «до/после» и значение ключа.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub kind: ChangeKind,
    pub relation_oid: u32,
    /// Образ «до»: полный кортеж (`O`), только ключевые колонки (`K`)
    /// либо `None`, если старого кортежа нет.
    pub old: Option<Value>,
    /// Образ «после» (всегда полный новый кортеж).
    pub new: Value,
    /// Значение ключа: из нового кортежа для Insert/Update, из старого для
    /// Delete. Составной ключ кодируется JSON-массивом строк.
    pub key: Option<String>,
}

/// Результат декодирования полезной нагрузки XLogData.
#[derive(Debug, Clone)]
pub enum DecodedEvent {
    /// Схема таблицы обновлена в кэше (OID возвращён для справки).
    Relation(u32),
    /// Изменение строки.
    Change(ChangeEvent),
    /// TRUNCATE: одна запись протокола может усечь несколько таблиц.
    Truncate(Vec<u32>),
    /// Сообщение, не влияющее на журнал (Type, Origin, потоковые служебные).
    Ignored,
}

/// Декодирует одно сообщение pgoutput и обновляет кэш отношений.
///
/// `data` — полезная нагрузка XLogData: тег-байт и тело. Begin/Commit и
/// логические MESSAGE на этом уровне не обрабатываются: их типизирует
/// транспорт (`pgwire-replication`).
pub fn decode(data: &[u8], cache: &mut RelationCache) -> Result<DecodedEvent> {
    let (&tag, body) = data.split_first().ok_or("пустое сообщение pgoutput")?;
    match tag {
        b'R' => decode_relation(body, cache),
        b'I' => decode_insert(body, cache),
        b'U' => decode_update(body, cache),
        b'D' => decode_delete(body, cache),
        b'T' => decode_truncate(body),
        b'B' | b'C' | b'M' => Err(format!(
            "граничные сообщения pgoutput ({}) обрабатывает транспорт",
            tag as char
        )
        .into()),
        _ => Ok(DecodedEvent::Ignored),
    }
}

fn take_i16(body: &mut &[u8]) -> Result<i16> {
    if body.len() < 2 {
        return Err("обрезанное i16 в сообщении pgoutput".into());
    }
    let (head, tail) = body.split_at(2);
    *body = tail;
    Ok(i16::from_be_bytes(head.try_into().expect("ровно 2 байта")))
}

fn take_i32(body: &mut &[u8]) -> Result<i32> {
    if body.len() < 4 {
        return Err("обрезанное i32 в сообщении pgoutput".into());
    }
    let (head, tail) = body.split_at(4);
    *body = tail;
    Ok(i32::from_be_bytes(head.try_into().expect("ровно 4 байта")))
}

fn take_cstr(body: &mut &[u8]) -> Result<String> {
    let end = body
        .iter()
        .position(|&b| b == 0)
        .ok_or("строка без терминатора в сообщении pgoutput")?;
    let s = std::str::from_utf8(&body[..end])
        .map_err(|e| format!("не-UTF8 строка в сообщении pgoutput: {e}"))?
        .to_owned();
    *body = &body[end + 1..];
    Ok(s)
}

fn take_oid(body: &mut &[u8]) -> Result<u32> {
    Ok(take_i32(body)? as u32)
}

/// Relation: oid(4), схема\0, таблица\0, [identity(1) — PG 17+], число
/// колонок(2), далее на колонку: флаги(1), имя\0, oid типа(4), typmod(4).
fn decode_relation(body: &[u8], cache: &mut RelationCache) -> Result<DecodedEvent> {
    let mut body = body;
    let oid = take_oid(&mut body)?;
    let schema = take_cstr(&mut body)?;
    let name = take_cstr(&mut body)?;
    // В PG 17+ перед колонками передаётся байт replica identity
    // (relreplident); в старших версиях — только с PG 17.
    let replica_identity = if cache.server_major() >= 17 {
        let (&byte, tail) = body.split_first().ok_or("Relation: нет байта identity")?;
        body = tail;
        ReplicaIdentity::from_byte(byte)
    } else {
        ReplicaIdentity::Default
    };
    let count = take_i16(&mut body)?;
    let mut columns = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        let flags = *body.first().ok_or("Relation: обрезанная колонка")?;
        body = &body[1..];
        let column_name = take_cstr(&mut body)?;
        let type_oid = take_oid(&mut body)?;
        let _typmod = take_i32(&mut body)?;
        columns.push(RelationColumn {
            name: column_name,
            type_oid,
            is_key: flags & 1 != 0,
        });
    }
    if !body.is_empty() {
        return Err("Relation: лишние байты в конце сообщения".into());
    }
    cache.register(Relation {
        oid,
        schema,
        name,
        replica_identity,
        columns,
        primary_key: None,
    });
    Ok(DecodedEvent::Relation(oid))
}

/// Кортеж: число колонок(2), далее на колонку: тег(1) и значение.
fn decode_tuple<'a>(body: &mut &'a [u8]) -> Result<Vec<Cell<'a>>> {
    let count = take_i16(body)?;
    let mut cells = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        let tag = *body.first().ok_or("кортеж: обрезанное значение")?;
        *body = &body[1..];
        let cell = match tag {
            b't' => {
                let len = take_i32(body)?;
                let len = usize::try_from(len).map_err(|_| "кортеж: отрицательная длина")?;
                if body.len() < len {
                    return Err("кортеж: значение короче заголовка".into());
                }
                let (head, tail) = body.split_at(len);
                *body = tail;
                Cell::Text(head)
            }
            b'n' => Cell::Null,
            b'u' => Cell::UnchangedToast,
            other => return Err(format!("кортеж: неизвестный тег значения {other:#x}").into()),
        };
        cells.push(cell);
    }
    Ok(cells)
}

/// JSON-образ кортежа по колонкам отношения.
/// Неизменённые TOAST-значения пропускаются (сервер их не прислал).
fn tuple_to_json(relation: &Relation, cells: &[Cell<'_>]) -> Result<Value> {
    if cells.len() != relation.columns.len() {
        return Err(format!(
            "кортеж: колонок {}, а в Relation {}",
            cells.len(),
            relation.columns.len()
        )
        .into());
    }
    let mut map = Map::with_capacity(cells.len());
    for (column, cell) in relation.columns.iter().zip(cells) {
        if matches!(cell, Cell::UnchangedToast) {
            continue;
        }
        map.insert(column.name.clone(), json_from_cell(column.type_oid, *cell));
    }
    Ok(Value::Object(map))
}

/// Значение ключа из кортежа по ключевым колонкам кэша.
///
/// Требует полного кортежа (позиции колонок совпадают с Relation);
/// для старого кортежа из `K` ключ считается иначе — см. `key_from_key_tuple`.
fn key_from_full_tuple(
    cache: &RelationCache,
    oid: u32,
    relation: &Relation,
    cells: &[Cell<'_>],
) -> Option<String> {
    let names = cache.key_columns(oid)?;
    let values: Option<Vec<String>> = names
        .iter()
        .map(|name| {
            relation
                .columns
                .iter()
                .position(|c| &c.name == name)
                .and_then(|index| cells.get(index))
                .and_then(|cell| match cell {
                    Cell::Text(raw) => Some(String::from_utf8_lossy(raw).into_owned()),
                    _ => None,
                })
        })
        .collect();
    key_string(values?)
}

/// Значение ключа из «ключевого» кортежа `K` (только identity-колонки).
/// JSON-образ в этом случае строится по флагам Relation.
fn key_from_key_tuple(relation: &Relation, cells: &[Cell<'_>]) -> Option<String> {
    let key_columns: Vec<&RelationColumn> = relation.columns.iter().filter(|c| c.is_key).collect();
    if key_columns.len() != cells.len() {
        return None;
    }
    let values: Vec<String> = key_columns
        .iter()
        .zip(cells)
        .filter_map(|(_, cell)| match cell {
            Cell::Text(raw) => Some(String::from_utf8_lossy(raw).into_owned()),
            _ => None,
        })
        .collect();
    if values.len() != cells.len() {
        return None;
    }
    key_string(values)
}

/// Один ключ — значение; составной — JSON-массив строк (для VARCHAR-колонки
/// журнала это однозначнее склейки разделителем).
fn key_string(values: Vec<String>) -> Option<String> {
    match values.len() {
        0 => None,
        1 => Some(values.into_iter().next()?),
        _ => Some(Value::Array(values.into_iter().map(Value::String).collect()).to_string()),
    }
}

/// Insert: oid(4), 'N', кортеж.
fn decode_insert(body: &[u8], cache: &RelationCache) -> Result<DecodedEvent> {
    let mut body = body;
    let oid = take_oid(&mut body)?;
    let relation = cache
        .relation(oid)
        .ok_or("Insert: Relation ещё не приходил")?;
    match body.first() {
        Some(b'N') => body = &body[1..],
        _ => return Err("Insert: ожидается новый кортеж 'N'".into()),
    }
    let cells = decode_tuple(&mut body)?;
    let new = tuple_to_json(relation, &cells)?;
    let key = key_from_full_tuple(cache, oid, relation, &cells);
    Ok(DecodedEvent::Change(ChangeEvent {
        kind: ChangeKind::Insert,
        relation_oid: oid,
        old: None,
        new,
        key,
    }))
}

/// Update: oid(4), затем либо ['O'|'K'] кортеж + 'N' кортеж, либо только 'N'.
fn decode_update(body: &[u8], cache: &RelationCache) -> Result<DecodedEvent> {
    let mut body = body;
    let oid = take_oid(&mut body)?;
    let relation = cache
        .relation(oid)
        .ok_or("Update: Relation ещё не приходил")?;

    let mut old = None;
    let mut old_key = None;
    if let Some(&tag @ (b'O' | b'K')) = body.first() {
        body = &body[1..];
        let cells = decode_tuple(&mut body)?;
        old = Some(tuple_to_json_keyed(relation, tag, &cells)?);
        old_key = if tag == b'K' {
            key_from_key_tuple(relation, &cells)
        } else {
            key_from_full_tuple(cache, oid, relation, &cells)
        };
    }

    match body.first() {
        Some(b'N') => body = &body[1..],
        _ => return Err("Update: ожидается новый кортеж 'N'".into()),
    }
    let cells = decode_tuple(&mut body)?;
    let new = tuple_to_json(relation, &cells)?;
    let key = old_key.or_else(|| key_from_full_tuple(cache, oid, relation, &cells));

    Ok(DecodedEvent::Change(ChangeEvent {
        kind: ChangeKind::Update,
        relation_oid: oid,
        old,
        new,
        key,
    }))
}

/// Delete: oid(4), 'O'|'K', кортеж.
fn decode_delete(body: &[u8], cache: &RelationCache) -> Result<DecodedEvent> {
    let mut body = body;
    let oid = take_oid(&mut body)?;
    let relation = cache
        .relation(oid)
        .ok_or("Delete: Relation ещё не приходил")?;
    let tag = *body.first().ok_or("Delete: нет кортежа")?;
    if tag != b'O' && tag != b'K' {
        return Err("Delete: ожидается старый кортеж 'O' или 'K'".into());
    }
    body = &body[1..];
    let cells = decode_tuple(&mut body)?;
    let old = tuple_to_json_keyed(relation, tag, &cells)?;
    let key = if tag == b'K' {
        key_from_key_tuple(relation, &cells)
    } else {
        key_from_full_tuple(cache, oid, relation, &cells)
    };

    Ok(DecodedEvent::Change(ChangeEvent {
        kind: ChangeKind::Delete,
        relation_oid: oid,
        old: Some(old),
        new: Value::Object(Map::new()),
        key,
    }))
}

/// Образ кортежа для старого значения: `O` — полный кортеж, `K` — только
/// ключевые колонки (так сервер отдаёт identity DEFAULT/INDEX).
fn tuple_to_json_keyed(relation: &Relation, tag: u8, cells: &[Cell<'_>]) -> Result<Value> {
    if tag == b'O' {
        return tuple_to_json(relation, cells);
    }
    // K-кортеж позиционирован по подмножеству identity-колонок: спариваем
    // его именно с ними, а не со всеми колонками Relation.
    let key_columns: Vec<&RelationColumn> = relation.columns.iter().filter(|c| c.is_key).collect();
    if key_columns.len() != cells.len() {
        return Ok(Value::Object(Map::new()));
    }
    let mut map = Map::with_capacity(cells.len());
    for (column, cell) in key_columns.iter().zip(cells) {
        if matches!(cell, Cell::UnchangedToast) {
            continue;
        }
        map.insert(column.name.clone(), json_from_cell(column.type_oid, *cell));
    }
    Ok(Value::Object(map))
}

/// Truncate: число таблиц(4), флаги(1), oid таблиц по числу.
fn decode_truncate(body: &[u8]) -> Result<DecodedEvent> {
    let mut body = body;
    let count = take_i32(&mut body)?;
    let count = usize::try_from(count).map_err(|_| "Truncate: отрицательное число таблиц")?;
    let _flags = *body.first().ok_or("Truncate: нет флагов")?;
    body = &body[1..];
    let mut oids = Vec::with_capacity(count);
    for _ in 0..count {
        oids.push(take_oid(&mut body)?);
    }
    if !body.is_empty() {
        return Err("Truncate: лишние байты в конце сообщения".into());
    }
    Ok(DecodedEvent::Truncate(oids))
}
