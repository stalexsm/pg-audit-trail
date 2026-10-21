use std::collections::HashMap;

/// Режим replica identity таблицы источника (байт из сообщения Relation,
/// PG 17+; совпадает с `pg_class.relreplident`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// `d` — ключевые колонки из первичного ключа (если он есть).
    Default,
    /// `n` — identity нет: UPDATE/DELETE такие таблицы не реплицируют.
    Nothing,
    /// `f` — весь старый кортеж; key-флаги помечают все колонки и ключ не определяют.
    Full,
    /// `i` — ключевые колонки из выбранного уникального индекса.
    Index,
}

impl ReplicaIdentity {
    pub(crate) fn from_byte(b: u8) -> Self {
        match b {
            b'd' => Self::Default,
            b'n' => Self::Nothing,
            b'f' => Self::Full,
            _ => Self::Index,
        }
    }
}

/// Колонка сообщения Relation.
#[derive(Debug, Clone)]
pub struct RelationColumn {
    pub name: String,
    /// OID типа из `pg_type` (16 = bool, 20 = int8, 25 = text, 1700 = numeric, ...).
    pub type_oid: u32,
    /// Колонка входит в replica identity (флаг 1 в сообщении).
    pub is_key: bool,
}

/// Схема таблицы источника из сообщения Relation.
#[derive(Debug, Clone)]
pub struct Relation {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<RelationColumn>,
    /// Ключевые колонки, полученные из каталога источника (фолбэк для FULL).
    /// Заполняется только через `RelationCache::set_primary_key`.
    pub(crate) primary_key: Option<Vec<String>>,
}

/// Кэш отношений: OID → схема таблицы.
///
/// Наполняется сообщениями Relation, которые сервер присылает перед первым
/// изменением таблицы в сессии декодирования и повторно после любой
/// инвалидации кэша (ALTER TABLE, TRUNCATE) — повторная регистрация
/// перезаписывает запись, поэтому смена схемы переживается без рестарта.
pub struct RelationCache {
    map: HashMap<u32, Relation>,
    /// Старший номер версии сервера-источника: в PG 17+ в Relation добавлен
    /// байт replica identity (см. `set_server_major`).
    server_major: u32,
}

impl RelationCache {
    pub fn new(server_major: u32) -> Self {
        Self {
            map: HashMap::new(),
            server_major,
        }
    }

    /// Старший номер версии сервера-источника (из `server_version_num`).
    /// Влияет на формат сообщения Relation: в PG 17+ после имени таблицы
    /// передаётся байт replica identity.
    pub fn set_server_major(&mut self, major: u32) {
        self.server_major = major;
    }

    pub fn server_major(&self) -> u32 {
        self.server_major
    }

    /// Регистрирует (перезаписывает) отношение и сбрасывает вычисленный
    /// по каталогу ключ — схема могла измениться.
    pub(crate) fn register(&mut self, relation: Relation) {
        self.map.insert(relation.oid, relation);
    }

    pub fn relation(&self, oid: u32) -> Option<&Relation> {
        self.map.get(&oid)
    }

    /// Запоминает имя ключевых колонок, полученное из каталога источника.
    ///
    /// Нужен только для `REPLICA IDENTITY FULL`: при ней key-флаги помечают
    /// все колонки старого кортежа и первичный ключ по ним не определить.
    pub fn set_primary_key(&mut self, oid: u32, columns: Vec<String>) {
        if let Some(relation) = self.map.get_mut(&oid) {
            relation.primary_key = (!columns.is_empty()).then_some(columns);
        }
    }

    /// Имена ключевых колонок: явный ключ из каталога либо флаги Relation.
    ///
    /// `None` — ключ неизвестен: у таблицы нет identity (или первичного
    /// ключа при FULL), либо Relation ещё не приходил.
    pub fn key_columns(&self, oid: u32) -> Option<Vec<String>> {
        let relation = self.map.get(&oid)?;
        match &relation.primary_key {
            Some(pk) => Some(pk.clone()),
            None if matches!(
                relation.replica_identity,
                ReplicaIdentity::Default | ReplicaIdentity::Index
            ) =>
            {
                let names: Vec<String> = relation
                    .columns
                    .iter()
                    .filter(|c| c.is_key)
                    .map(|c| c.name.clone())
                    .collect();
                (!names.is_empty()).then_some(names)
            }
            None => None,
        }
    }
}
