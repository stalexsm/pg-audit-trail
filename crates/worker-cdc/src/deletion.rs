//! Исполнение удаления источника (ADR-0005, тикет #31): DELETE помечает
//! источник (`deleted_at`) и отвечает 202, воркер исполняет удаление —
//! дропает у клиента слот и публикацию, затем снимает строку реестра
//! и чекпоинт дропнутого слота. Клиент недоступен — повтор с backoff;
//! состояние застрявшего удаления видно в реестре (`deleting` с текстом
//! ошибки), пока дроп не удался. Строки журнала удалённого источника в
//! `changes` не трогаются: журнал переживает источник.

use std::time::Duration;

use anyhow::Context;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::Result;
use crate::backoff;
use crate::config::{DB_ACQUIRE_TIMEOUT_SECS, WorkerConfig};
use crate::pool;
use crate::registry::{self, SourceRecord, StatusWriter};

/// Размер пула удаления к БД аудита: статус застрявшего удаления и снятие
/// строки — короткие одиночные запросы.
const DELETION_AUDIT_POOL_MAX_CONNECTIONS: u32 = 1;

/// Размер пула к клиенту: дроп слота и публикации — по одной операции.
const DELETION_SOURCE_POOL_MAX_CONNECTIONS: u32 = 1;

/// Исполняет удаление источника до успеха или остановки воркера.
///
/// Каждая попытка: подключение к клиенту, дроп слота и публикации, снятие
/// чекпоинта и строки реестра. Повторные попытки безопасны: несуществующие
/// слот и публикация пропускаются, отсутствующие строки чекпоинта и реестра
/// — тоже успех. Неудача — статус `deleting` с текстом ошибки в реестре и
/// повтор с backoff: строка реестра не снимается, пока дроп у клиента не
/// удался, иначе брошенный слот бесконечно копил бы WAL (ADR-0001, ADR-0005).
pub(crate) async fn run(
    config: WorkerConfig,
    record: SourceRecord,
    token: CancellationToken,
    mut supervisor_gone: watch::Receiver<()>,
) {
    // Пул к БД аудита принадлежит исполнению удаления: статус застрявшего
    // удаления и снятие строк не делят соединения с супервизором.
    let Some(audit) = pool::audit_pool_with_retry(
        &config,
        DELETION_AUDIT_POOL_MAX_CONNECTIONS,
        &record.name,
        "исполнение удаления",
        &token,
        &mut supervisor_gone,
    )
    .await
    else {
        return;
    };
    let status = StatusWriter::new(&audit, record.name.clone());

    status.deleting(None).await;
    info!(
        source = %record.name,
        slot = %record.slot(),
        publication = %record.publication(),
        "исполняем удаление источника: дроп слота и публикации у клиента"
    );

    // Счётчик подряд идущих неудач удаления: задаёт экспоненту backoff.
    let mut attempt: u32 = 0;
    loop {
        if token.is_cancelled() || supervisor_gone.has_changed().is_err() {
            return;
        }
        tokio::select! {
            _ = token.cancelled() => return,
            // Супервизор исчез (аварийная остановка): удаление остаётся
            // помеченным в реестре, исполнение продолжит новый воркер.
            _ = supervisor_gone.changed() => return,
            outcome = run_attempt(&record, &audit) => {
                match outcome {
                    Ok(()) => {
                        info!(
                            source = %record.name,
                            "удаление источника исполнено: слот и публикация дропнуты, строка реестра снята"
                        );
                        return;
                    }
                    Err(e) => {
                        // Состояние застрявшего удаления видно в реестре,
                        // пока дроп не удался.
                        status.deleting(Some(&e)).await;
                        attempt += 1;
                        warn!(
                            source = %record.name,
                            attempt,
                            reason = "дроп у клиента не удался — повтор с backoff",
                            error = %e,
                        );
                        if !backoff::pause(
                            &token,
                            &mut supervisor_gone,
                            backoff::reconnect_delay(&config, attempt),
                        )
                        .await
                        {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Одна попытка удаления: подключение к клиенту, дроп слота и публикации,
/// затем снятие чекпоинта и строки реестра. Дроп идёт до снятия строки:
/// строка — единственная память о незакрытом слоте, без неё повтор был
/// бы невозможен.
async fn run_attempt(record: &SourceRecord, audit: &PgPool) -> Result<()> {
    // Пул клиента живёт одну попытку: пересоздаётся после обрыва.
    let source = PgPoolOptions::new()
        .max_connections(DELETION_SOURCE_POOL_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(DB_ACQUIRE_TIMEOUT_SECS))
        .connect(&record.url)
        .await
        .context("подключение к БД клиента")?;

    // Слот дропается только если существует: волатильная функция в списке
    // выборки не вычисляется для отфильтрованных WHERE строк. Занятый
    // другим консюмером слот даёт ошибку — попытка повторится.
    sqlx::query(
        "SELECT pg_drop_replication_slot(slot_name) \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(record.slot())
    .execute(&source)
    .await
    .context("дроп слота репликации")?;

    // Имя публикации деривировано из имени источника, проверенного
    // правилом слотов [a-z0-9_] (CHECK в схеме реестра) — инъекции нет.
    let publication = record.publication();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP PUBLICATION IF EXISTS \"{publication}\""
    )))
    .execute(&source)
    .await
    .context("дроп публикации")?;

    // Чекпоинт дропнутого слота и строка реестра снимаются после дропа;
    // строки журнала не трогаются.
    registry::delete_checkpoint(audit, &record.slot()).await?;
    registry::delete_row(audit, &record.name).await?;
    Ok(())
}
