//! Консюмер журнала: логическая репликация источника → таблица `changes` БД
//! аудита (ADR-0001).
//!
//! Транспорт репликации — `pgwire-replication`, декодирование pgoutput —
//! крейт `pgoutput`. Тонкий `main` поверх библиотеки: `run` применяется
//! миграциями к БД аудита, бутстрапит слот и чекпоинт, крутит цикл
//! репликации до отмены `CancellationToken`.

pub mod bootstrap;
pub mod config;
pub mod ingest;
pub mod replicate;

/// Псевдоним ошибок крейта по конвенции репозитория (AGENTS.md).
pub(crate) type Result<T, E = anyhow::Error> = core::result::Result<T, E>;

pub use config::WorkerConfig;
pub use replicate::run;
