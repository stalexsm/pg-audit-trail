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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // Фикстуры — сырые байты сообщений pgoutput (PostgreSQL, гл. 53.9
    // «Logical Replication Message Formats»), без PostgreSQL.

    /// Таблица users(pub), oid 50: id int8 — ключевая, name text.
    /// Формат PG 17+: после имени таблицы идёт байт replica identity.
    const RELATION_USERS_PG17: &[u8] = &[
        b'R', // тег сообщения
        0, 0, 0, 50, // oid отношения = 50
        b'p', b'u', b'b', 0, // схема "pub"
        b'u', b's', b'e', b'r', b's', 0,    // таблица "users"
        b'd', // replica identity: default
        0, 2, // колонок: 2
        // id: флаг 1 (входит в identity), тип int8 (20), typmod -1
        1, b'i', b'd', 0, 0, 0, 0, 20, 0xFF, 0xFF, 0xFF, 0xFF,
        // name: неключевая, тип text (25), typmod -1
        0, b'n', b'a', b'm', b'e', 0, 0, 0, 0, 25, 0xFF, 0xFF, 0xFF, 0xFF,
    ];

    /// Та же таблица в формате PG 16: без байта replica identity.
    const RELATION_USERS_PG16: &[u8] = &[
        b'R', 0, 0, 0, 50, b'p', b'u', b'b', 0, b'u', b's', b'e', b'r', b's', 0, 0, 2, 1, b'i',
        b'd', 0, 0, 0, 0, 20, 0xFF, 0xFF, 0xFF, 0xFF, 0, b'n', b'a', b'm', b'e', 0, 0, 0, 0, 25,
        0xFF, 0xFF, 0xFF, 0xFF,
    ];

    /// Таблица evts(pub), oid 60: составной ключ id int8 + tenant text.
    const RELATION_EVTS_PG17: &[u8] = &[
        b'R', 0, 0, 0, 60, b'p', b'u', b'b', 0, b'e', b'v', b't', b's', 0, b'd', 0, 2, 1, b'i',
        b'd', 0, 0, 0, 0, 20, 0xFF, 0xFF, 0xFF, 0xFF, 1, b't', b'e', b'n', b'a', b'n', b't', 0, 0,
        0, 0, 25, 0xFF, 0xFF, 0xFF, 0xFF,
    ];

    /// oid таблицы users для тел сообщений.
    const USERS_OID: [u8; 4] = [0, 0, 0, 50];

    /// Новый кортеж users: id = 7, name = "Anna".
    const TUPLE_NEW_ANNA: &[u8] = &[
        0, 2, // колонок: 2
        b't', 0, 0, 0, 1, b'7', b't', 0, 0, 0, 4, b'A', b'n', b'n', b'a',
    ];

    /// Старый кортеж users: id = 7, name = "Old".
    const TUPLE_OLD: &[u8] = &[
        0, 2, b't', 0, 0, 0, 1, b'7', b't', 0, 0, 0, 3, b'O', b'l', b'd',
    ];

    /// Ключевой кортеж K: только identity-колонка id = 7.
    const TUPLE_KEY_ID: &[u8] = &[0, 1, b't', 0, 0, 0, 1, b'7'];

    /// Позиция байта replica identity в RELATION_USERS_PG17:
    /// тег (1) + oid (4) + "pub\0" (4) + "users\0" (6).
    const IDENTITY_BYTE_INDEX: usize = 15;

    /// Фикстура Relation users с подставленным байтом replica identity.
    fn users_relation_with_identity(byte: u8) -> Vec<u8> {
        let mut relation = RELATION_USERS_PG17.to_vec();
        relation[IDENTITY_BYTE_INDEX] = byte;
        relation
    }

    /// Клеит сообщение из тега и кусков тела — читаемость составных фикстур.
    fn message(tag: u8, parts: &[&[u8]]) -> Vec<u8> {
        let mut msg = Vec::with_capacity(1 + parts.iter().map(|p| p.len()).sum::<usize>());
        msg.push(tag);
        for part in parts {
            msg.extend_from_slice(part);
        }
        msg
    }

    /// Проверяет, что сообщение не декодируется, и фиксирует фрагмент ошибки.
    fn assert_decode_error(data: &[u8], cache: &mut RelationCache, fragment: &str) {
        let err = decode(data, cache).expect_err("сообщение обязано не декодироваться");
        assert!(
            err.to_string().contains(fragment),
            "ожидался фрагмент «{fragment}», получено: {err}"
        );
    }

    /// Достаёт изменение из результата декодирования.
    fn expect_change(data: &[u8], cache: &mut RelationCache) -> ChangeEvent {
        match decode(data, cache).expect("сообщение обязано декодироваться")
        {
            DecodedEvent::Change(change) => change,
            other => panic!("ожидалось Change, получено: {other:?}"),
        }
    }

    /// Кэш сервера PG 17+ с зарегистрированной таблицей users (oid 50).
    fn cache_with_users() -> RelationCache {
        let mut cache = RelationCache::new(17);
        decode(RELATION_USERS_PG17, &mut cache).expect("Relation обязан декодироваться");
        cache
    }

    // --- Успешное декодирование: по фикстуре на тип сообщения ---

    #[test]
    fn relation_registers_schema() {
        let mut cache = RelationCache::new(17);
        let event = decode(RELATION_USERS_PG17, &mut cache).expect("должно декодироваться");
        assert!(matches!(event, DecodedEvent::Relation(50)));
        let relation = cache
            .relation(50)
            .expect("отношение обязано появиться в кэше");
        assert_eq!(relation.schema, "pub");
        assert_eq!(relation.name, "users");
        assert_eq!(relation.replica_identity, ReplicaIdentity::Default);
        assert_eq!(relation.columns.len(), 2);
        assert!(relation.columns[0].is_key);
        assert!(!relation.columns[1].is_key);
        assert_eq!(relation.columns[0].name, "id");
        assert_eq!(relation.columns[0].type_oid, 20);
        assert_eq!(relation.columns[1].name, "name");
    }

    #[test]
    fn relation_of_server_16_has_no_identity_byte() {
        let mut cache = RelationCache::new(16);
        let event = decode(RELATION_USERS_PG16, &mut cache).expect("должно декодироваться");
        assert!(matches!(event, DecodedEvent::Relation(50)));
        let relation = cache.relation(50).expect("отношение в кэше");
        assert_eq!(relation.replica_identity, ReplicaIdentity::Default);
    }

    #[test]
    fn relation_reads_identity_byte_from_wire() {
        let mut cache = RelationCache::new(17);
        // Та же фикстура с байтом 'f' (FULL) на месте replica identity.
        let full = users_relation_with_identity(b'f');
        decode(&full, &mut cache).expect("должно декодироваться");
        let relation = cache.relation(50).expect("отношение в кэше");
        assert_eq!(relation.replica_identity, ReplicaIdentity::Full);
    }

    #[test]
    fn insert_decodes_tuple_and_key() {
        let mut cache = cache_with_users();
        let msg = message(b'I', &[&USERS_OID, b"N", TUPLE_NEW_ANNA]);
        let change = expect_change(&msg, &mut cache);
        assert_eq!(change.kind, ChangeKind::Insert);
        assert_eq!(change.relation_oid, 50);
        assert!(change.old.is_none());
        assert_eq!(change.new, json!({"id": 7, "name": "Anna"}));
        assert_eq!(change.key.as_deref(), Some("7"));
    }

    #[test]
    fn update_with_full_old_tuple() {
        let mut cache = cache_with_users();
        let msg = message(b'U', &[&USERS_OID, b"O", TUPLE_OLD, b"N", TUPLE_NEW_ANNA]);
        let change = expect_change(&msg, &mut cache);
        assert_eq!(change.kind, ChangeKind::Update);
        assert_eq!(change.old, Some(json!({"id": 7, "name": "Old"})));
        assert_eq!(change.new, json!({"id": 7, "name": "Anna"}));
        assert_eq!(change.key.as_deref(), Some("7"));
    }

    #[test]
    fn update_with_key_tuple_k() {
        let mut cache = cache_with_users();
        let msg = message(
            b'U',
            &[&USERS_OID, b"K", TUPLE_KEY_ID, b"N", TUPLE_NEW_ANNA],
        );
        let change = expect_change(&msg, &mut cache);
        // K-кортеж образует образ только из identity-колонок.
        assert_eq!(change.old, Some(json!({"id": 7})));
        assert_eq!(change.key.as_deref(), Some("7"));
    }

    #[test]
    fn update_without_old_tuple() {
        let mut cache = cache_with_users();
        let msg = message(b'U', &[&USERS_OID, b"N", TUPLE_NEW_ANNA]);
        let change = expect_change(&msg, &mut cache);
        assert!(change.old.is_none());
        // Ключ берётся из нового кортежа.
        assert_eq!(change.key.as_deref(), Some("7"));
    }

    #[test]
    fn delete_decodes_old_and_key_tuples() {
        for (tag, tuple, expected_old) in [
            (&b'O', TUPLE_OLD, json!({"id": 7, "name": "Old"})),
            (&b'K', TUPLE_KEY_ID, json!({"id": 7})),
        ] {
            let mut cache = cache_with_users();
            let msg = message(b'D', &[&USERS_OID, &[*tag], tuple]);
            let change = expect_change(&msg, &mut cache);
            assert_eq!(change.kind, ChangeKind::Delete);
            assert_eq!(change.old, Some(expected_old));
            assert_eq!(change.new, json!({}));
            // Ключ удаления — из старого кортежа.
            assert_eq!(change.key.as_deref(), Some("7"));
        }
    }

    #[test]
    fn truncate_lists_all_tables() {
        let mut cache = RelationCache::new(17);
        let body = [0, 0, 0, 2, 0x00, 0, 0, 0, 1, 0, 0, 0, 50];
        let msg = message(b'T', &[&body]);
        let event = decode(&msg, &mut cache).expect("должно декодироваться");
        assert!(matches!(event, DecodedEvent::Truncate(oids) if oids == vec![1, 50]));
    }

    #[test]
    fn unknown_tags_are_ignored() {
        for tag in *b"YOwz" {
            let mut cache = RelationCache::new(17);
            let event = decode(&[tag], &mut cache).expect("незнакомый тег не должен ронять парсер");
            assert!(matches!(event, DecodedEvent::Ignored));
        }
    }

    // --- Значения кортежа: текст, NULL, unchanged-toast, пустая строка ---

    #[test]
    fn unchanged_toast_column_is_skipped() {
        let mut cache = cache_with_users();
        // name пришёл как 'u': колонки нет в образе (не null).
        let tuple = [0, 2, b't', 0, 0, 0, 1, b'7', b'u'];
        let msg = message(b'I', &[&USERS_OID, b"N", &tuple]);
        let change = expect_change(&msg, &mut cache);
        assert_eq!(change.new, json!({"id": 7}));
    }

    #[test]
    fn null_cell_becomes_json_null() {
        let mut cache = cache_with_users();
        let tuple = [0, 2, b't', 0, 0, 0, 1, b'7', b'n'];
        let msg = message(b'I', &[&USERS_OID, b"N", &tuple]);
        let change = expect_change(&msg, &mut cache);
        assert_eq!(change.new, json!({"id": 7, "name": null}));
    }

    #[test]
    fn empty_text_value_is_kept() {
        let mut cache = cache_with_users();
        let tuple = [0, 2, b't', 0, 0, 0, 1, b'7', b't', 0, 0, 0, 0];
        let msg = message(b'I', &[&USERS_OID, b"N", &tuple]);
        let change = expect_change(&msg, &mut cache);
        assert_eq!(change.new, json!({"id": 7, "name": ""}));
    }

    // --- Значение ключа: составной ключ, identity и каталог ---

    #[test]
    fn composite_key_becomes_json_array() {
        let mut cache = RelationCache::new(17);
        decode(RELATION_EVTS_PG17, &mut cache).expect("Relation обязан декодироваться");
        let tuple = [0, 2, b't', 0, 0, 0, 1, b'7', b't', 0, 0, 0, 2, b'r', b'u'];
        let msg = message(b'I', &[&[0, 0, 0, 60], b"N", &tuple]);
        let change = expect_change(&msg, &mut cache);
        assert_eq!(change.key.as_deref(), Some("[\"7\",\"ru\"]"));
    }

    #[test]
    fn key_is_absent_without_replica_identity() {
        // Таблица с identity 'n': ключа у изменений нет.
        let mut cache = RelationCache::new(17);
        let nothing = users_relation_with_identity(b'n');
        decode(&nothing, &mut cache).expect("должно декодироваться");
        let msg = message(b'I', &[&USERS_OID, b"N", TUPLE_NEW_ANNA]);
        let change = expect_change(&msg, &mut cache);
        assert!(change.key.is_none());
    }

    #[test]
    fn full_identity_takes_key_from_catalog() {
        let mut cache = RelationCache::new(17);
        let full = users_relation_with_identity(b'f');
        decode(&full, &mut cache).expect("должно декодироваться");
        let msg = message(b'I', &[&USERS_OID, b"N", TUPLE_NEW_ANNA]);
        // Без каталога ключ при FULL не определить.
        assert!(expect_change(&msg, &mut cache).key.is_none());
        // Ключевые колонки пришли из каталога источника.
        cache.set_primary_key(50, vec!["id".to_owned()]);
        assert_eq!(expect_change(&msg, &mut cache).key.as_deref(), Some("7"));
    }

    #[test]
    fn key_tuple_with_wrong_cell_count_builds_empty_image() {
        let mut cache = cache_with_users();
        // K-кортеж с двумя ячейками против одной identity-колонки:
        // образ пуст, ключ считается по новому кортежу.
        let k_tuple = [0, 2, b't', 0, 0, 0, 1, b'7', b't', 0, 0, 0, 2, b'r', b'u'];
        let msg = message(b'U', &[&USERS_OID, b"K", &k_tuple, b"N", TUPLE_NEW_ANNA]);
        let change = expect_change(&msg, &mut cache);
        assert_eq!(change.old, Some(json!({})));
        assert_eq!(change.key.as_deref(), Some("7"));
    }

    // --- Ветки ошибок: по фиксатору на каждую ---

    #[test]
    fn empty_message_is_an_error() {
        let mut cache = RelationCache::new(17);
        assert_decode_error(&[], &mut cache, "пустое сообщение pgoutput");
    }

    #[test]
    fn boundary_tags_belong_to_transport() {
        for tag in *b"BCM" {
            let mut cache = RelationCache::new(17);
            assert_decode_error(&[tag], &mut cache, "обрабатывает транспорт");
        }
    }

    #[test]
    fn truncated_i16_is_reported() {
        let mut cache = RelationCache::new(16);
        // Relation: после имени таблицы один байт вместо числа колонок.
        let msg = message(b'R', &[&[0, 0, 0, 50], b"s\0", b"t\0", &[0]]);
        assert_decode_error(&msg, &mut cache, "обрезанное i16");
        // Кортеж: число колонок обрезано.
        let mut cache = cache_with_users();
        let msg = message(b'I', &[&USERS_OID, b"N", &[0]]);
        assert_decode_error(&msg, &mut cache, "обрезанное i16");
    }

    #[test]
    fn truncated_i32_is_reported() {
        let mut cache = RelationCache::new(17);
        // Insert: oid обрезан до двух байт.
        assert_decode_error(&[b'I', 0, 0], &mut cache, "обрезанное i32");
        // Truncate: после флагов нет oid таблицы.
        let msg = message(b'T', &[&[0, 0, 0, 1, 0]]);
        assert_decode_error(&msg, &mut cache, "обрезанное i32");
    }

    #[test]
    fn string_without_terminator_is_reported() {
        let mut cache = RelationCache::new(16);
        let msg = message(b'R', &[&[0, 0, 0, 50], b"ab"]);
        assert_decode_error(&msg, &mut cache, "строка без терминатора");
    }

    #[test]
    fn non_utf8_string_is_reported() {
        let mut cache = RelationCache::new(16);
        let msg = message(b'R', &[&[0, 0, 0, 50], &[0xFF, 0]]);
        assert_decode_error(&msg, &mut cache, "не-UTF8 строка");
    }

    #[test]
    fn relation_on_server_17_requires_identity_byte() {
        // Тело заканчивается сразу после имени таблицы: байту identity
        // взяться неоткуда.
        let mut cache = RelationCache::new(17);
        let msg = message(b'R', &[&[0, 0, 0, 50], b"pub\0", b"users\0"]);
        assert_decode_error(&msg, &mut cache, "нет байта identity");
    }

    #[test]
    fn relation_with_missing_column_is_reported() {
        let mut cache = RelationCache::new(17);
        // Заявлено 2 колонки, пришла только первая.
        let msg = message(
            b'R',
            &[
                &[0, 0, 0, 50],
                b"pub\0",
                b"users\0",
                &[b'd', 0, 2],
                &[1, b'i', b'd', 0, 0, 0, 0, 20, 0xFF, 0xFF, 0xFF, 0xFF],
            ],
        );
        assert_decode_error(&msg, &mut cache, "обрезанная колонка");
    }

    #[test]
    fn relation_with_trailing_bytes_is_reported() {
        let mut cache = RelationCache::new(17);
        let mut msg = RELATION_USERS_PG17.to_vec();
        msg.push(0xAA);
        assert_decode_error(&msg, &mut cache, "лишние байты");
    }

    #[test]
    fn tuple_without_tag_byte_is_reported() {
        let mut cache = cache_with_users();
        let msg = message(b'I', &[&USERS_OID, b"N", &[0, 1]]);
        assert_decode_error(&msg, &mut cache, "кортеж: обрезанное значение");
    }

    #[test]
    fn tuple_with_negative_length_is_reported() {
        let mut cache = cache_with_users();
        let msg = message(
            b'I',
            &[&USERS_OID, b"N", &[0, 1, b't', 0xFF, 0xFF, 0xFF, 0xFF]],
        );
        assert_decode_error(&msg, &mut cache, "отрицательная длина");
    }

    #[test]
    fn tuple_shorter_than_header_is_reported() {
        let mut cache = cache_with_users();
        // Заявлено 9 байт значения, пришло 3.
        let msg = message(
            b'I',
            &[
                &USERS_OID,
                b"N",
                &[0, 1, b't', 0, 0, 0, 9, b'o', b'n', b'e'],
            ],
        );
        assert_decode_error(&msg, &mut cache, "значение короче заголовка");
    }

    #[test]
    fn tuple_with_unknown_cell_tag_is_reported() {
        let mut cache = cache_with_users();
        let msg = message(b'I', &[&USERS_OID, b"N", &[0, 1, 0x7F]]);
        assert_decode_error(&msg, &mut cache, "неизвестный тег значения");
    }

    #[test]
    fn tuple_wider_than_relation_is_reported() {
        let mut cache = cache_with_users();
        // Ячейка одна, а в Relation две колонки.
        let msg = message(b'I', &[&USERS_OID, b"N", &[0, 1, b't', 0, 0, 0, 1, b'7']]);
        assert_decode_error(&msg, &mut cache, "колонок 1, а в Relation 2");
    }

    #[test]
    fn insert_without_relation_is_reported() {
        let mut cache = RelationCache::new(17);
        let msg = message(b'I', &[&USERS_OID, b"N", TUPLE_NEW_ANNA]);
        assert_decode_error(&msg, &mut cache, "Insert: Relation ещё не приходил");
    }

    #[test]
    fn insert_with_wrong_tuple_tag_is_reported() {
        let mut cache = cache_with_users();
        let msg = message(b'I', &[&USERS_OID, b"O", TUPLE_NEW_ANNA]);
        assert_decode_error(&msg, &mut cache, "Insert: ожидается новый кортеж 'N'");
        // Тег-байта нет вовсе — та же ветка.
        let msg = message(b'I', &[&USERS_OID]);
        assert_decode_error(&msg, &mut cache, "Insert: ожидается новый кортеж 'N'");
    }

    #[test]
    fn update_without_relation_is_reported() {
        let mut cache = RelationCache::new(17);
        let msg = message(b'U', &[&USERS_OID, b"N", TUPLE_NEW_ANNA]);
        assert_decode_error(&msg, &mut cache, "Update: Relation ещё не приходил");
    }

    #[test]
    fn update_without_new_tuple_is_reported() {
        let mut cache = cache_with_users();
        // После старого кортежа 'O' новый кортеж 'N' не пришёл.
        let msg = message(b'U', &[&USERS_OID, b"O", TUPLE_OLD]);
        assert_decode_error(&msg, &mut cache, "Update: ожидается новый кортеж 'N'");
        // Нет и старого кортежа.
        let msg = message(b'U', &[&USERS_OID]);
        assert_decode_error(&msg, &mut cache, "Update: ожидается новый кортеж 'N'");
    }

    #[test]
    fn delete_without_relation_is_reported() {
        let mut cache = RelationCache::new(17);
        let msg = message(b'D', &[&USERS_OID, b"O", TUPLE_OLD]);
        assert_decode_error(&msg, &mut cache, "Delete: Relation ещё не приходил");
    }

    #[test]
    fn delete_without_tuple_is_reported() {
        let mut cache = cache_with_users();
        let msg = message(b'D', &[&USERS_OID]);
        assert_decode_error(&msg, &mut cache, "Delete: нет кортежа");
    }

    #[test]
    fn delete_with_wrong_tuple_tag_is_reported() {
        let mut cache = cache_with_users();
        let msg = message(b'D', &[&USERS_OID, b"N"]);
        assert_decode_error(
            &msg,
            &mut cache,
            "Delete: ожидается старый кортеж 'O' или 'K'",
        );
    }

    #[test]
    fn truncate_with_negative_count_is_reported() {
        let mut cache = RelationCache::new(17);
        let msg = message(b'T', &[&[0xFF, 0xFF, 0xFF, 0xFF, 0x00]]);
        assert_decode_error(&msg, &mut cache, "отрицательное число таблиц");
    }

    #[test]
    fn truncate_without_flags_is_reported() {
        let mut cache = RelationCache::new(17);
        let msg = message(b'T', &[&[0, 0, 0, 1]]);
        assert_decode_error(&msg, &mut cache, "нет флагов");
    }

    #[test]
    fn truncate_with_trailing_bytes_is_reported() {
        let mut cache = RelationCache::new(17);
        let msg = message(b'T', &[&[0, 0, 0, 1, 0x00, 0, 0, 0, 50, 0xAA]]);
        assert_decode_error(&msg, &mut cache, "лишние байты");
    }
}
