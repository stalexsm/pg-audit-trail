//! Пулы соединений с повтором создания: задачи источников и исполнения
//! удаления создают свои пулы к БД аудита (изоляция потребителей — батчи
//! и статусы не делят соединения). Пока БД аудита недоступна, создание
//! повторяется с backoff до успеха или остановки.

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::backoff;
use crate::config::{DB_ACQUIRE_TIMEOUT_SECS, WorkerConfig};

/// Создаёт пул к БД аудита, повторяя попытку до остановки (токен воркера
/// или исчезновение супервизора). `None` — остановка пришла в паузе между
/// попытками: владелец пула не запускается.
pub(crate) async fn audit_pool_with_retry(
    config: &WorkerConfig,
    max_connections: u32,
    source: &str,
    purpose: &str,
    token: &CancellationToken,
    supervisor_gone: &mut watch::Receiver<()>,
) -> Option<PgPool> {
    loop {
        match PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(DB_ACQUIRE_TIMEOUT_SECS))
            .connect(&config.database_url)
            .await
        {
            Ok(pool) => return Some(pool),
            Err(e) => {
                warn!(source, purpose, error = %e, "пул к БД аудита не создан — повтор");
                if !backoff::pause(token, supervisor_gone, backoff::reconnect_delay(config, 0))
                    .await
                {
                    return None;
                }
            }
        }
    }
}
