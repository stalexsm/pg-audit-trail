use std::time::Duration;

use crate::Result;
use crate::bootstrap::{self, Bootstrap};
use crate::config::WorkerConfig;
use crate::ingest::{self, ChangeRow, TransactionBuffer};
use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use pgoutput::{ChangeKind, DecodedEvent, Relation, RelationCache, ReplicaIdentity, decode};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Эпоха PostgreSQL: метки времени протокола репликации — микросекунды
/// с 2000-01-01 00:00:00 UTC.
const PG_EPOCH_SECONDS: i64 = 946_684_800;

/// Запускает воркер CDC до отмены `CancellationToken`.
///
/// Применяет миграции к БД аудита, бутстрапит слот и чекпоинт, читает поток
/// репликации и вставляет события в `changes` батчами в одной транзакции
/// с чекпоинтом; слот подтверждается только после коммита вставки.
pub async fn run(config: WorkerConfig, token: CancellationToken) -> Result<()> {
    let audit = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&config.database_url)
        .await
        .context("подключение к БД аудита")?;
    sqlx::migrate!("../../migrations")
        .run(&audit)
        .await
        .context("миграции БД аудита")?;

    // Пул источника живёт весь цикл: по нему читается каталог для
    // первичных ключей таблиц с REPLICA IDENTITY FULL.
    let source_pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&config.source_database_url)
        .await
        .context("подключение к источнику CDC")?;
    let boot = bootstrap::bootstrap(&audit, &source_pool, &config.slot).await?;
    info!(
        slot = %config.slot,
        publication = %config.publication,
        source_database = %boot.source_database,
        start_lsn = %boot.start_lsn,
        "бутстрап завершён, запускаем репликацию"
    );

    let mut client = connect_replication(&config, boot.start_lsn).await?;
    client
        .wait_started()
        .await
        .context("старт репликации не подтверждён сервером")?;
    info!("репликация запущена");

    let mut cache = RelationCache::new(boot.server_major);
    let mut buffer: Option<TransactionBuffer> = None;

    let result = loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("получен сигнал остановки: незакоммиченная транзакция будет переиграна слотом");
                break Ok(());
            }
            event = client.recv() => match event {
                Ok(Some(ReplicationEvent::Begin { xid, .. })) => {
                    buffer = Some(TransactionBuffer::new(xid));
                }
                Ok(Some(ReplicationEvent::XLogData { wal_start, data, .. })) => {
                    let Some(buf) = buffer.as_mut() else {
                        warn!("изменение вне транзакции (Begin не приходил) — пропущено");
                        continue;
                    };
                    if let Err(e) =
                        handle_xlog(&source_pool, &boot, &config, &mut cache, buf, &data, wal_start)
                            .await
                    {
                        break Err(e);
                    }
                }
                Ok(Some(ReplicationEvent::Message { transactional, prefix, lsn, content, .. })) => {
                    // Событие приложения (ADR-0002) на этом этапе игнорируется:
                    // строки с операцией MESSAGE — тикет #8. Индекс расходуется,
                    // чтобы нумерация событий транзакции оставалась детерминированной
                    // для replay-дедупликации.
                    if transactional
                        && let Some(buf) = buffer.as_mut()
                    {
                        buf.next_index();
                    }
                    info!(
                        %prefix, %lsn, %transactional, bytes = content.len(),
                        "MESSAGE пропущен (события приложения — тикет #8)"
                    );
                }
                Ok(Some(ReplicationEvent::Commit { end_lsn, commit_time_micros, .. })) => {
                    let Some(mut buf) = buffer.take() else {
                        continue;
                    };
                    buf.set_committed_at(pg_micros_to_utc(commit_time_micros));
                    let rows = buf.take_rows();
                    match ingest::flush(
                        &audit,
                        &rows,
                        &config.slot,
                        end_lsn.as_u64() as i64,
                        config.batch_size,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!(rows = rows.len(), end_lsn = %end_lsn, "транзакция источника сохранена в журнал");
                            // Ack слоту строго после коммита транзакции вставки.
                            client.update_applied_lsn(end_lsn);
                        }
                        Err(e) => break Err(e),
                    }
                }
                // KeepAlive: ответы и статусные апдейты транспорт берёт на себя.
                Ok(Some(_)) => {}
                Ok(None) => {
                    warn!("поток репликации закрыт сервером");
                    break Ok(());
                }
                Err(e) => break Err(anyhow::anyhow!("ошибка потока репликации: {e}")),
            }
        }
    };

    if let Err(e) = client.shutdown().await {
        warn!(error = %e, "штатное закрытие репликации завершилось с ошибкой");
    }
    result
}

/// Подключение к walsender источника: `START_REPLICATION … messages 'true'`.
async fn connect_replication(config: &WorkerConfig, start_lsn: Lsn) -> Result<ReplicationClient> {
    let url = url::Url::parse(&config.source_database_url).context("разбор SOURCE_DATABASE_URL")?;
    let host = url
        .host_str()
        .context("в SOURCE_DATABASE_URL нет хоста")?
        .to_owned();
    let port = url.port_or_known_default().context("порт источника")?;
    let user = if url.username().is_empty() {
        "postgres".to_owned()
    } else {
        url.username().to_owned()
    };
    let password = url.password().unwrap_or_default().to_owned();
    let database = url.path().trim_start_matches('/').to_owned();

    let repl_config = ReplicationConfig::new(
        host,
        user,
        password,
        database,
        config.slot.clone(),
        config.publication.clone(),
    )
    .with_port(port)
    .with_start_lsn(start_lsn);

    let client = ReplicationClient::connect(repl_config)
        .await
        .context("подключение к walsender источника")?;
    Ok(client)
}

/// Декодирует одно XLogData-сообщение и готовит строки журнала.
async fn handle_xlog(
    source: &PgPool,
    boot: &Bootstrap,
    config: &WorkerConfig,
    cache: &mut RelationCache,
    buf: &mut TransactionBuffer,
    data: &[u8],
    wal_start: Lsn,
) -> Result<()> {
    match decode(data, cache).map_err(|e| anyhow::anyhow!("{e}"))? {
        DecodedEvent::Relation(oid) => {
            // Повторный Relation (ALTER TABLE, TRUNCATE) перезаписывает запись
            // кэша. Ключ ищем в каталоге источника, когда key-флаги ключа
            // не дают: при REPLICA IDENTITY FULL флаги помечают все колонки;
            // на PG 14–16 identity-байта в Relation нет (считаем Default),
            // и FULL выдаёт себя теми же флагами на всех колонках.
            let needs_pk_lookup = cache.relation(oid).is_some_and(|rel| {
                rel.replica_identity == ReplicaIdentity::Full
                    || (rel.replica_identity == ReplicaIdentity::Default
                        && rel.columns.len() > 1
                        && rel.columns.iter().all(|c| c.is_key))
            });
            if needs_pk_lookup {
                let pk = bootstrap::primary_key_columns(source, oid).await?;
                cache.set_primary_key(oid, pk);
            }
        }
        DecodedEvent::Change(change) => {
            let event_index = buf.next_index();
            let relation = cache
                .relation(change.relation_oid)
                .with_context(|| format!("нет Relation для OID {}", change.relation_oid))?;
            if ingest::is_ignored(&config.ignore_tables, &relation.schema, &relation.name) {
                return Ok(());
            }

            let (primary_key, before, after) = match change.kind {
                ChangeKind::Insert => (change.key.clone(), json!({}), change.new.clone()),
                ChangeKind::Update => (
                    change.key.clone(),
                    change.old.clone().unwrap_or_else(|| json!({})),
                    change.new.clone(),
                ),
                ChangeKind::Delete => (
                    change.key.clone(),
                    change.old.clone().unwrap_or_else(|| json!({})),
                    json!({}),
                ),
            };

            buf.push_row(journal_row(
                boot,
                relation,
                ingest::operation_name(change.kind),
                primary_key,
                before,
                after,
                wal_start,
                event_index,
                buf.xid,
            ));
        }
        DecodedEvent::Truncate(relation_oids) => {
            for oid in relation_oids {
                let event_index = buf.next_index();
                let relation = cache
                    .relation(oid)
                    .with_context(|| format!("Truncate: нет Relation для OID {oid}"))?;
                if ingest::is_ignored(&config.ignore_tables, &relation.schema, &relation.name) {
                    continue;
                }
                buf.push_row(journal_row(
                    boot,
                    relation,
                    "TRUNCATE",
                    None,
                    json!({}),
                    json!({}),
                    wal_start,
                    event_index,
                    buf.xid,
                ));
            }
        }
        DecodedEvent::Ignored => {}
    }
    Ok(())
}

/// Строка журнала из декодированного события: метаданные источника,
/// время чтения из WAL и LSN события (`position`).
#[allow(clippy::too_many_arguments)] // все поля — данные одного события
fn journal_row(
    boot: &Bootstrap,
    relation: &Relation,
    operation: &'static str,
    primary_key: Option<String>,
    before: Value,
    after: Value,
    wal_start: Lsn,
    event_index: i32,
    xid: i64,
) -> ChangeRow {
    ChangeRow {
        database: boot.source_database.clone(),
        schema: relation.schema.clone(),
        table: relation.name.clone(),
        primary_key,
        operation,
        before,
        after,
        committed_at: Utc::now(), // перезаписывается временем коммита
        queued_at: Utc::now(),
        transaction_id: xid,
        position: wal_start.as_u64() as i64, // LSN события: u64 влезает в i64
        event_index,
    }
}

/// Микросекунды от эпохи PostgreSQL → `DateTime<Utc>`.
fn pg_micros_to_utc(micros: i64) -> DateTime<Utc> {
    let seconds = PG_EPOCH_SECONDS + micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    Utc.timestamp_opt(seconds, nanos)
        .single()
        .unwrap_or_else(Utc::now)
}
