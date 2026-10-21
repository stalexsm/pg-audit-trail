//! Парсер протокола логической репликации PostgreSQL pgoutput.
//!
//! Декодирует полезную нагрузку XLogData (тег + байты сообщения) в изменения
//! данных и служебные сообщения: Relation (кэш отношений), Insert, Update,
//! Delete, Truncate. Граничные сообщения Begin/Commit и логические MESSAGE
//! на транспортном уровне типизирует крейт `pgwire-replication`, поэтому
//! сюда они не доходят.
//!
//! Формат сообщений: PostgreSQL, гл. 53.9 «Logical Replication Message Formats».
//! Значения кортежей приходят в текстовом представлении (DateStyle ISO
//! фиксируется логическим декодированием), конвертация в JSON — по OID типа
//! из сообщения Relation.

mod decode;
mod error;
mod relation;
mod value;

pub use decode::{Cell, ChangeEvent, ChangeKind, DecodedEvent, decode};
pub use error::PgOutputError;
pub use relation::{Relation, RelationCache, RelationColumn, ReplicaIdentity};

pub(crate) type Result<T> = core::result::Result<T, PgOutputError>;
