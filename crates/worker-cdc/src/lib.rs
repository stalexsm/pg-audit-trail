//! Консюмер журнала: логическая репликация источников → таблица `changes`
//! БД аудита (ADR-0001, ADR-0005).
//!
//! Транспорт репликации — `pgwire-replication`, декодирование pgoutput —
//! крейт `pgoutput`. Тонкий `main` поверх библиотеки: супервизор применяется
//! миграциями к БД аудита, опрашивает реестр источников и держит по задаче
//! на каждый источник (свой слот, публикация, чекпоинт и статус в реестре)
//! до отмены `CancellationToken`.

mod backoff;
mod bootstrap;
mod config;
mod deletion;
mod ingest;
mod metrics;
mod pool;
mod registry;
mod replicate;
mod source_task;
mod supervisor;

/// Псевдоним ошибок крейта по конвенции репозитория (AGENTS.md).
pub(crate) type Result<T, E = anyhow::Error> = core::result::Result<T, E>;

pub use config::WorkerConfig;
pub use supervisor::run;
