use std::time::{Duration, Instant};

use crate::Result;
use crate::bootstrap::{self, Bootstrap};
use crate::config::WorkerConfig;
use crate::ingest::{self, ChangeRow, TransactionBuffer};
use crate::metrics;
use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use pgoutput::{ChangeKind, DecodedEvent, Relation, RelationCache, ReplicaIdentity, decode};
use pgwire_replication::error::PgWireError;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Эпоха PostgreSQL: метки времени протокола репликации — микросекунды
/// с 2000-01-01 00:00:00 UTC.
const PG_EPOCH_SECONDS: i64 = 946_684_800;

/// Префикс `application_name` walsender-соединения: по имени
/// `waltrail-cdc-<слот>` соединение опознаётся в `pg_stat_replication`
/// (мониторинг оператором и интеграционные тесты). Сервер обрезает
/// application_name до 63 байт (NAMEDATALEN) — длинные имена слотов
/// в статистике будут с «хвостом на срез».
const APPLICATION_NAME_PREFIX: &str = "waltrail-cdc";

/// Размер пула к БД аудита: вставки батчами + чтение чекпоинта.
const AUDIT_POOL_MAX_CONNECTIONS: u32 = 5;

/// Размер пула к источнику: только чтение каталога (первичные ключи).
const SOURCE_POOL_MAX_CONNECTIONS: u32 = 2;

/// Таймаут взятия соединения из пулов, секунды.
const DB_ACQUIRE_TIMEOUT_SECS: u64 = 30;

/// Чем закончилась сессия репликации — одно подключение к walsender.
enum SessionEnd {
    /// Остановка по `CancellationToken`: штатный выход из `run` без реконнекта.
    Cancelled,
    /// Сессия прервалась (обрыв соединения, ошибка потока, закрытие сервером):
    /// реконнект с backoff и продолжением с чекпоинта.
    Reconnect(anyhow::Error),
}

/// Запускает воркер CDC до отмены `CancellationToken`.
///
/// Применяет миграции к БД аудита, бутстрапит слот и чекпоинт, читает поток
/// репликации и вставляет события в `changes` батчами в одной транзакции
/// с чекпоинтом; слот подтверждается только после коммита вставки.
///
/// Воркер переживает сбои и рестарты (тикет #7): истина старта — чекпоинт
/// в БД аудита, каждая сессия стартует от меньшей из координат (чекпоинт,
/// `confirmed_flush_lsn` слота); прерванная сессия сменяется реконнектом
/// с экспоненциальным backoff, replay-дубли гасятся дедупликацией по
/// `UNIQUE(commit_lsn, event_index)`. Нелечимые ошибки конфигурации
/// (неверные креды, TLS) завершают воркер с ошибкой.
pub async fn run(config: WorkerConfig, token: CancellationToken) -> Result<()> {
    let audit = PgPoolOptions::new()
        .max_connections(AUDIT_POOL_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(DB_ACQUIRE_TIMEOUT_SECS))
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
        .max_connections(SOURCE_POOL_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(DB_ACQUIRE_TIMEOUT_SECS))
        .connect(&config.source_database_url)
        .await
        .context("подключение к источнику CDC")?;

    // Счётчик подряд идущих неудач подключения: сбрасывается каждой
    // удавшейся сессией, задаёт экспоненту backoff.
    let mut attempt: u32 = 0;

    // Сводные метрики конвейера (walsender → intake → вставка) пишутся
    // в лог до остановки воркера.
    let reporter = tokio::spawn(metrics::report_loop(token.clone()));

    let result: Result<()> = loop {
        if token.is_cancelled() {
            info!("остановка запрошена до подключения: воркер завершён");
            break Ok(());
        }

        // Чекпоинт перечитывается перед каждой сессией: после сбоя
        // продолжение идёт именно с него, а не с позиций прошлой сессии.
        let boot = match bootstrap::bootstrap(&audit, &source_pool, &config.slot).await {
            Ok(boot) => boot,
            Err(e) => {
                attempt += 1;
                if !backoff_after_failure(&token, &config, attempt, "бутстрап не удался", &e).await
                {
                    break Ok(());
                }
                continue;
            }
        };
        info!(
            slot = %config.slot,
            publication = %config.publication,
            source_database = %boot.source_database,
            start_lsn = %boot.start_lsn,
            "бутстрап завершён, подключаемся к репликации"
        );

        let mut client = match connect_replication(&config, boot.start_lsn).await {
            Ok(client) => client,
            Err(e) if has_fatal_config_error(&e) => break Err(e),
            Err(e) => {
                attempt += 1;
                if !backoff_after_failure(&token, &config, attempt, "подключение не удалось", &e)
                    .await
                {
                    break Ok(());
                }
                continue;
            }
        };
        if let Err(e) = client.wait_started().await {
            let err = anyhow::Error::new(e).context("старт репликации не подтверждён сервером");
            if has_fatal_config_error(&err) {
                break Err(err);
            }
            // Например, слот занят другим консюмером: ждём освобождения.
            attempt += 1;
            if !backoff_after_failure(&token, &config, attempt, "старт не подтверждён", &err).await
            {
                break Ok(());
            }
            continue;
        }
        // Подключение и старт удались: счётчик неудач сбрасывается.
        attempt = 0;
        info!("репликация запущена");

        let end = stream_session(&mut client, &audit, &source_pool, &boot, &config, &token).await;
        if matches!(end, SessionEnd::Cancelled)
            && let Err(e) = client.shutdown().await
        {
            warn!(error = %e, "штатное закрытие репликации завершилось с ошибкой");
        }
        match end {
            SessionEnd::Cancelled => {
                info!("воркер остановлен по CancellationToken: позиции сохранены чекпоинтом");
                break Ok(());
            }
            SessionEnd::Reconnect(e) => {
                attempt += 1;
                if !backoff_after_failure(
                    &token,
                    &config,
                    attempt,
                    "сессия репликации прервана — продолжение с чекпоинта",
                    &e,
                )
                .await
                {
                    break Ok(());
                }
            }
        }
    };

    reporter.abort();
    result
}

/// Одна сессия репликации: чтение потока до отмены, ошибки или закрытия
/// сервером. Кэш отношений и буфер транзакции живут внутри сессии —
/// новое подключение получает Relation заново, недоконченная транзакция
/// переигрывается слотом от последнего чекпоинта.
async fn stream_session(
    client: &mut ReplicationClient,
    audit: &PgPool,
    source_pool: &PgPool,
    boot: &Bootstrap,
    config: &WorkerConfig,
    token: &CancellationToken,
) -> SessionEnd {
    let mut cache = RelationCache::new(boot.server_major);
    let mut buffer: Option<TransactionBuffer> = None;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("получен сигнал остановки: незакоммиченная транзакция будет переиграна слотом");
                return SessionEnd::Cancelled;
            }
            event = client.recv() => match event {
                Ok(Some(ReplicationEvent::Begin { xid, .. })) => {
                    buffer = Some(TransactionBuffer::new(xid));
                }
                Ok(Some(ReplicationEvent::XLogData { wal_start, data, .. })) => {
                    metrics::add_wire(data.len());
                    let Some(buf) = buffer.as_mut() else {
                        warn!("изменение вне транзакции (Begin не приходил) — пропущено");
                        continue;
                    };
                    // Чистое время ступени intake: декодирование и сборка строк.
                    let intake_started = Instant::now();
                    let outcome =
                        handle_xlog(source_pool, boot, config, &mut cache, buf, &data, wal_start)
                            .await;
                    metrics::add_intake_time(intake_started.elapsed());
                    if let Err(e) = outcome {
                        return SessionEnd::Reconnect(e);
                    }
                }
                Ok(Some(ReplicationEvent::Message { transactional, prefix, lsn, content, .. })) => {
                    metrics::add_wire(content.len());
                    if !transactional {
                        debug!(%prefix, %lsn, bytes = content.len(), "внетранзакционный MESSAGE пропущен");
                        continue;
                    }
                    let Some(buf) = buffer.as_mut() else {
                        debug!(%prefix, %lsn, bytes = content.len(), "MESSAGE вне Begin пропущен");
                        continue;
                    };
                    // Индекс расходуется на любой транзакционный MESSAGE,
                    // даже если строку журнала он не создаёт: нумерация
                    // событий транзакции детерминирована потоком WAL —
                    // на этом держится replay-дедупликация.
                    buf.next_index();
                    // Только MESSAGE с настроенным префиксом (по умолчанию
                    // `_audit`) интерпретируется как канал контекста
                    // (ADR-0004): чужие префиксы строкам контекст не меняют.
                    if prefix == config.message_prefix {
                        let intake_started = Instant::now();
                        handle_audit_message(buf, &content, lsn);
                        metrics::add_intake_time(intake_started.elapsed());
                    }
                }
                Ok(Some(ReplicationEvent::Commit { end_lsn, commit_time_micros, .. })) => {
                    let Some(mut buf) = buffer.take() else {
                        continue;
                    };
                    buf.set_committed_at(pg_micros_to_utc(commit_time_micros));
                    // Контекст последнего MESSAGE (ADR-0004) проставляется
                    // строкам изменений строго перед вставкой батча;
                    // транзакция без изменений строк вставки не порождает,
                    // но чекпоинт продвигает.
                    buf.apply_transaction_context();
                    let rows = buf.take_rows();
                    let flush_started = Instant::now();
                    match ingest::flush(
                        audit,
                        &rows,
                        &config.slot,
                        end_lsn.as_u64() as i64,
                        config.batch_size,
                    )
                    .await
                    {
                        Ok(()) => {
                            metrics::add_flush(flush_started.elapsed(), rows.len());
                            info!(rows = rows.len(), end_lsn = %end_lsn, "транзакция источника сохранена в журнал");
                            // Ack слоту строго после коммита транзакции вставки.
                            client.update_applied_lsn(end_lsn);
                        }
                        Err(e) => return SessionEnd::Reconnect(e.context("вставка транзакции в журнал")),
                    }
                }
                // KeepAlive: ответы и статусные апдейты транспорт берёт на себя.
                Ok(Some(_)) => {}
                Ok(None) => {
                    return SessionEnd::Reconnect(anyhow::anyhow!("поток репликации закрыт сервером"));
                }
                Err(e) => return SessionEnd::Reconnect(anyhow::anyhow!("ошибка потока репликации: {e}")),
            }
        }
    }
}

/// Задержка перед (пере)подключением по номеру подряд идущей попытки.
fn reconnect_delay(config: &WorkerConfig, attempt: u32) -> Duration {
    backoff_delay(
        Duration::from_millis(config.reconnect_initial_backoff_ms),
        Duration::from_millis(config.reconnect_max_backoff_ms),
        attempt,
    )
}

/// Единая реакция на неудачную попытку: warn-лог с причиной и пауза backoff,
/// прерываемая остановкой воркера. `false` — сигнал остановки пришёл в паузе.
async fn backoff_after_failure(
    token: &CancellationToken,
    config: &WorkerConfig,
    attempt: u32,
    reason: &str,
    error: &anyhow::Error,
) -> bool {
    warn!(attempt, reason, error = %error, "повтор с backoff");
    wait_reconnect(token, reconnect_delay(config, attempt)).await
}

/// Экспоненциальный backoff `initial * 2^attempt`, ограниченный сверху `max`;
/// ниже `initial` не опускается даже при `max < initial`.
fn backoff_delay(initial: Duration, max: Duration, attempt: u32) -> Duration {
    // Потолок степени защищает от переполнения при больших номерах попыток.
    let doubling = initial.saturating_mul(2u32.saturating_pow(attempt.min(30)));
    doubling.min(max).max(initial)
}

/// Пауза перед реконнектом, прерываемая остановкой воркера:
/// `false` — пока шла пауза, поступил сигнал остановки.
async fn wait_reconnect(token: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = token.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// Ошибка подключения не лечится реконнектом: неверные креды или TLS-конфиг.
/// Остальное (обрывы, занятый слот, перезапуск источника) — повторять.
fn has_fatal_config_error(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<PgWireError>())
        .any(|e| e.is_auth() || e.is_tls())
}

/// Подключение к walsender источника: `START_REPLICATION … messages 'true'`.
///
/// Обязательство спеки «отвечать на все keepalive с reply_requested и слать
/// периодический standby status update» лежит на транспорте pgwire-replication:
/// он отвечает на каждый серверный keepalive и отправляет статус-апдейт в
/// пределах `status_interval` (в том числе на молчащем потоке); позиция для
/// апдейтов — `update_applied_lsn` после коммита вставки.
///
/// Подтверждаемая слоту позиция (`ack_lsn`) — точка старта сессии: слот
/// не продвигается дальше durable-чекпоинта, пока вставки не закоммичены
/// и не вызван `update_applied_lsn`.
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

    let mut repl_config = ReplicationConfig::new(
        host,
        user,
        password,
        database,
        config.slot.clone(),
        config.publication.clone(),
    )
    .with_port(port)
    .with_start_lsn(start_lsn);
    // Durable-позиция воркера: слот не подтверждается дальше чекпоинта.
    repl_config.ack_lsn = Some(start_lsn);
    // Имя соединения для pg_stat_replication: мониторинг и тесты.
    repl_config.application_name = Some(format!("{APPLICATION_NAME_PREFIX}-{}", config.slot));

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
            metrics::add_event();
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
                metrics::add_event();
            }
        }
        DecodedEvent::Ignored => {}
    }
    Ok(())
}

/// Обрабатывает MESSAGE префикса `_audit` (ADR-0004): payload целиком
/// становится контекстом транзакции, строковое значение ключа `trace_id`
/// верхнего уровня — колонкой и далее всеми строками этой транзакции.
/// Строку журнала MESSAGE не создаёт: единственное содержимое журнала —
/// изменения строк. Контрактом payload является JSON: не-JSON payload
/// порождает warn и контекста не меняет, на поток репликации это не
/// влияет (индекс события уже потрачен).
fn handle_audit_message(buf: &mut TransactionBuffer, content: &[u8], lsn: Lsn) {
    let payload: Value = match std::str::from_utf8(content)
        .map_err(|e| anyhow::anyhow!("не-UTF8 payload: {e}"))
        .and_then(|text| {
            serde_json::from_str(text).map_err(|e| anyhow::anyhow!("payload не JSON: {e}"))
        }) {
        Ok(payload) => payload,
        Err(e) => {
            warn!(%lsn, error = %e, "payload MESSAGE пропущен: контракт — JSON (ADR-0004)");
            return;
        }
    };
    if payload
        .get(ingest::TRACE_ID_KEY)
        .is_some_and(|value| !value.is_string())
    {
        warn!(%lsn, "ключ trace_id в payload MESSAGE не строка — в колонку не попал");
    }
    let trace_id = ingest::trace_id_from_payload(&payload);
    buf.push_message(payload, trace_id);
    metrics::add_event();
    debug!(%lsn, "MESSAGE принят как контекст транзакции");
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
        // Контекст последнего MESSAGE проставляется при коммите (ADR-0004).
        context: json!({}),
        committed_at: Utc::now(), // перезаписывается временем коммита
        queued_at: Utc::now(),
        transaction_id: xid,
        position: wal_start.as_u64() as i64, // LSN события: u64 влезает в i64
        event_index,
        trace_id: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_starts_at_initial_and_doubles() {
        let initial = Duration::from_millis(200);
        let max = Duration::from_secs(30);
        assert_eq!(backoff_delay(initial, max, 0), initial);
        assert_eq!(backoff_delay(initial, max, 1), Duration::from_millis(400));
        assert_eq!(backoff_delay(initial, max, 2), Duration::from_millis(800));
        assert_eq!(backoff_delay(initial, max, 3), Duration::from_millis(1600));
    }

    #[test]
    fn backoff_is_capped_at_max() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(30);
        assert_eq!(backoff_delay(initial, max, 5), max);
        assert_eq!(
            backoff_delay(initial, max, 100),
            max,
            "переполнение степени не должно пробить верхнюю границу"
        );
    }

    #[test]
    fn backoff_floor_is_initial_even_when_max_is_smaller() {
        let initial = Duration::from_secs(5);
        assert_eq!(
            backoff_delay(initial, Duration::from_secs(1), 3),
            initial,
            "нулевая пауза реконнекта недопустима"
        );
    }
}
