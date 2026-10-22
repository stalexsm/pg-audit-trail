use std::time::Instant;

use crate::Result;
use crate::bootstrap::{self, Bootstrap};
use crate::ingest::{self, TransactionBuffer};
use crate::metrics;
use crate::registry::{SourceRecord, StatusWriter};
use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use pgoutput::{ChangeKind, DecodedEvent, Relation, RelationCache, ReplicaIdentity, decode};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};
use serde_json::{Value, json};
use sqlx::PgPool;
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

/// Чем закончилась сессия репликации — одно подключение к walsender.
pub(crate) enum SessionEnd {
    /// Остановка по `CancellationToken`: штатный выход задачи без реконнекта.
    Cancelled,
    /// Сессия прервалась (обрыв соединения, ошибка потока, закрытие сервером):
    /// реконнект с backoff и продолжением с чекпоинта.
    Reconnect(anyhow::Error),
}

/// Одна сессия репликации источника: чтение потока до отмены, ошибки или
/// закрытия сервером. Кэш отношений и буфер транзакции живут внутри сессии —
/// новое подключение получает Relation заново, недоконченная транзакция
/// переигрывается слотом от последнего чекпоинта.
pub(crate) async fn stream_session(
    client: &mut ReplicationClient,
    audit: &PgPool,
    source_pool: &PgPool,
    boot: &Bootstrap,
    record: &SourceRecord,
    status: &StatusWriter<'_>,
    token: &CancellationToken,
) -> SessionEnd {
    let mut cache = RelationCache::new(boot.server_major);
    let mut buffer: Option<TransactionBuffer> = None;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!(source = %record.name, "получен сигнал остановки: незакоммиченная транзакция будет переиграна слотом");
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
                        handle_xlog(source_pool, boot, record, &mut cache, buf, &data, wal_start)
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
                    // Только MESSAGE с префиксом из реестра источника
                    // (исторический дефолт `_audit`) интерпретируется как
                    // канал контекста (ADR-0004): чужие префиксы строкам
                    // контекст не меняют.
                    if prefix == record.message_prefix {
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
                        &record.slot(),
                        end_lsn.as_u64() as i64,
                        record.batch_size,
                    )
                    .await
                    {
                        Ok(()) => {
                            metrics::add_flush(flush_started.elapsed(), rows.len());
                            info!(source = %record.name, rows = rows.len(), end_lsn = %end_lsn, "транзакция источника сохранена в журнал");
                            // Статус источника: время последнего события
                            // (обновляется с троттлингом, ошибку не прокидывает).
                            status.event_saved().await;
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

/// Координаты и креды источника, разобранные из URL подключения.
pub(crate) struct SourceEndpoint {
    /// Хост источника.
    pub(crate) host: String,
    /// Порт источника.
    pub(crate) port: u16,
    /// Пользователь (percent-decoded).
    pub(crate) user: String,
    /// Пароль (percent-decoded), пустая строка — пароля в URL нет.
    pub(crate) password: String,
    /// Имя базы (percent-decoded).
    pub(crate) database: String,
}

/// Разбирает URL источника в эндпоинт для подключения: user/password/имя
/// базы декодируются из percent-encoding. Крейт `url` хранит компоненты
/// закодированными как есть, а sqlx при разборе той же строки декодирует
/// (`PgConnectOptions::from_str` → `percent_decode_str`): без декодирования
/// репликационный тракт с паролем со спецсимволами (@, :, /, %) не проходит
/// аутентификацию walsender, тогда как бутстрап по той же строке подключается
/// (тикет #43). Для декодирования — крейт `percent-encoding`: каноничная
/// зависимость самого `url`, уже присутствующая в дереве.
/// Хост и порт не декодируются: `url` нормализует их сам (IDNA, число).
pub(crate) fn endpoint_from_url(url: &str) -> Result<SourceEndpoint> {
    let url = url::Url::parse(url).context("разбор URL источника")?;
    let host = url
        .host_str()
        .context("в URL источника нет хоста")?
        .to_owned();
    let port = url.port_or_known_default().context("порт источника")?;
    let user = if url.username().is_empty() {
        "postgres".to_owned()
    } else {
        percent_decode(url.username(), "имени пользователя")?
    };
    let password = match url.password() {
        Some(raw) => percent_decode(raw, "пароля")?,
        None => String::new(),
    };
    let database = percent_decode(url.path().trim_start_matches('/'), "имени базы")?;
    Ok(SourceEndpoint {
        host,
        port,
        user,
        password,
        database,
    })
}

/// Декодирует один percent-encoded компонент URL; невалидная UTF-8
/// последовательность после декодирования — ошибка разбора источника.
fn percent_decode(raw: &str, what: &str) -> Result<String> {
    percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .with_context(|| format!("декодирование {what} из URL источника"))
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
pub(crate) async fn connect_replication(
    record: &SourceRecord,
    start_lsn: Lsn,
) -> Result<ReplicationClient> {
    let slot = record.slot();
    let endpoint = endpoint_from_url(&record.url)?;

    let mut repl_config = ReplicationConfig::new(
        endpoint.host,
        endpoint.user,
        endpoint.password,
        endpoint.database,
        slot.clone(),
        record.publication(),
    )
    .with_port(endpoint.port)
    .with_start_lsn(start_lsn);
    // Durable-позиция воркера: слот не подтверждается дальше чекпоинта.
    repl_config.ack_lsn = Some(start_lsn);
    // Имя соединения для pg_stat_replication: мониторинг и тесты.
    repl_config.application_name = Some(format!("{APPLICATION_NAME_PREFIX}-{slot}"));

    let client = ReplicationClient::connect(repl_config)
        .await
        .context("подключение к walsender источника")?;
    Ok(client)
}

/// Декодирует одно XLogData-сообщение и готовит строки журнала.
async fn handle_xlog(
    source: &PgPool,
    boot: &Bootstrap,
    record: &SourceRecord,
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
            if ingest::is_ignored(&record.ignore_tables, &relation.schema, &relation.name) {
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
                record,
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
                if ingest::is_ignored(&record.ignore_tables, &relation.schema, &relation.name) {
                    continue;
                }
                buf.push_row(journal_row(
                    boot,
                    record,
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

/// Обрабатывает MESSAGE настроенного префикса источника (из реестра,
/// по умолчанию `_audit`) как канал контекста транзакции (ADR-0004):
/// payload целиком становится контекстом транзакции, строковое значение
/// ключа `trace_id` верхнего уровня — колонкой и далее всеми строками этой
/// транзакции. Строку журнала MESSAGE не создаёт: единственное содержимое
/// журнала — изменения строк. Контрактом payload является JSON: не-JSON
/// payload порождает warn и контекста не меняет, на поток репликации это
/// не влияет (индекс события уже потрачен).
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
    buf.push_message(payload);
    metrics::add_event();
    debug!(%lsn, "MESSAGE принят как контекст транзакции");
}

/// Строка журнала из декодированного события: метаданные источника,
/// время чтения из WAL и LSN события (`position`).
#[allow(clippy::too_many_arguments)] // все поля — данные одного события
fn journal_row(
    boot: &Bootstrap,
    record: &SourceRecord,
    relation: &Relation,
    operation: &'static str,
    primary_key: Option<String>,
    before: Value,
    after: Value,
    wal_start: Lsn,
    event_index: i32,
    xid: i64,
) -> ingest::ChangeRow {
    ingest::ChangeRow {
        // Ключ источника из реестра: дедуп-ключи журнала включают источник.
        source: record.name.clone(),
        database: boot.source_database.clone(),
        schema: relation.schema.clone(),
        table: relation.name.clone(),
        // Ключ обрезается под ширину колонки (VARCHAR(255)) по границе
        // символов: слишком длинный ключ уронил бы всю транзакцию вставки.
        primary_key: ingest::truncate_primary_key(primary_key),
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

    /// Строка журнала с заданными метаданными источника для тестов.
    fn journal_row_fixture(source: &str, database: &str) -> ingest::ChangeRow {
        ingest::ChangeRow {
            source: source.to_owned(),
            database: database.to_owned(),
            schema: "public".into(),
            table: "t".into(),
            primary_key: None,
            operation: "CREATE",
            before: json!({}),
            after: json!({}),
            context: json!({}),
            committed_at: Utc::now(),
            queued_at: Utc::now(),
            transaction_id: 1,
            position: 1,
            event_index: 0,
            trace_id: None,
        }
    }

    /// Строки журнала одного источника несут его ключ в колонке `source`:
    /// дедуп-ключи журнала включают источник, чтение фильтруется по нему.
    #[test]
    fn journal_rows_carry_source_key() {
        let row = journal_row_fixture("client_a", "clientdb");
        assert_eq!(row.source, "client_a");
        assert_eq!(row.database, "clientdb");

        let other = journal_row_fixture("client_b", "otherdb");
        assert_ne!(row.source, other.source);
    }

    /// Креды и имя базы декодируются из percent-encoding: пароль со
    /// спецсимволами (@, :, /, %) и имя пользователя (`:` в позиции username
    /// обязан кодироваться), закодированные в URL источника, обязаны дойти
    /// до аутентификации walsender в исходном виде — так же, как их
    /// видит sqlx-тракт при разборе той же строки (тикет #43).
    #[test]
    fn endpoint_decodes_percent_encoded_credentials() {
        let endpoint = endpoint_from_url(
            "postgresql://repl%3Aops:p%40ss%3Aw%2Frd%25@db.example:5433/clien%2Ft%20db",
        )
        .expect("разбор URL источника");
        assert_eq!(endpoint.host, "db.example");
        assert_eq!(endpoint.port, 5433);
        assert_eq!(endpoint.user, "repl:ops");
        assert_eq!(endpoint.password, "p@ss:w/rd%");
        assert_eq!(endpoint.database, "clien/t db");
    }

    /// Без кредов в URL — дефолтный пользователь postgres и пустой пароль
    /// (прежнее поведение репликационного тракта).
    #[test]
    fn endpoint_defaults_without_credentials() {
        let endpoint = endpoint_from_url("postgresql://db.example:5433/clientdb")
            .expect("разбор URL источника");
        assert_eq!(endpoint.host, "db.example");
        assert_eq!(endpoint.port, 5433);
        assert_eq!(endpoint.user, "postgres");
        assert_eq!(endpoint.password, "");
        assert_eq!(endpoint.database, "clientdb");
    }
}
