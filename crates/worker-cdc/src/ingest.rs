use chrono::{DateTime, Utc};
use pgoutput::ChangeKind;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::QueryBuilder;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::Result;

/// Ключ payload MESSAGE с идентификатором сквозной операции (ADR-0004).
pub const TRACE_ID_KEY: &str = "trace_id";

/// Предел длины `trace_id` под колонку `trace_id` VARCHAR(255).
///
/// VARCHAR считает символы, а режем по байтам — это сознательно
/// консервативно: строка из ≤255 байт всегда влезает в VARCHAR(255),
/// даже если состоит из многобайтных символов. Без обрезки вставка
/// слишком длинного значения уронила бы всю транзакцию вставки.
pub const MAX_TRACE_ID_LEN: usize = 255;

/// Лимит протокола PostgreSQL на параметры одного extended-запроса: 65535
/// на statement (UInt16 в wire-протоколе); превышение — ошибка протокола,
/// соединение рвётся.
const MAX_STATEMENT_PARAMS: usize = 65_535;

/// Число bind-параметров на одну строку журнала в INSERT `changes`
/// (`flush_once`: по колонке на каждое поле `ChangeRow`, включая LSN
/// коммита, общий для всех строк чанка).
const CHANGE_ROW_PARAMS: usize = 16;

/// Максимум строк журнала в одном statement — бюджет параметров протокола
/// в пересчёте на строку (65535 / 16 = 4095). Верхняя граница `batch_size`
/// в API совпадает с этим пределом; чанки вставки не превышают его, даже
/// если в реестре значение больше (правка в обход API, исторические записи).
const MAX_ROWS_PER_STATEMENT: usize = MAX_STATEMENT_PARAMS / CHANGE_ROW_PARAMS;

// Инвариант бюджета: полный чанк обязан влезать в лимит statement.
const _: () = assert!(CHANGE_ROW_PARAMS * MAX_ROWS_PER_STATEMENT <= MAX_STATEMENT_PARAMS);

/// Базовая пауза между повторами вставки батча, миллисекунды: растёт
/// линейно с номером попытки (200, 400, …).
const RETRY_BASE_DELAY_MS: u64 = 200;

/// Разделяемый пустой контекст: дефолт строк до первого MESSAGE — без
/// аллокации на каждую строку (один Arc на все строки без MESSAGE).
static EMPTY_CONTEXT: LazyLock<Arc<Value>> = LazyLock::new(|| Arc::new(json!({})));

/// Пустой контекст транзакции: общий Arc для строк без MESSAGE.
pub(crate) fn empty_context() -> Arc<Value> {
    Arc::clone(&EMPTY_CONTEXT)
}

/// Предел длины `primary_key` под колонку `primary_key` VARCHAR(255).
///
/// Тот же подход, что и для `trace_id`: VARCHAR считает символы, а режем
/// по байтам — строка из ≤255 байт всегда влезает в VARCHAR(255). Ключ
/// формируется в pgoutput (составной — JSON-массивом), поэтому обрезается
/// итоговое закодированное значение уже на шве вставки в журнал. Без
/// обрезки вставка слишком длинного ключа уронила бы всю транзакцию
/// вставки: транзакция-«отравитель» зацикливала бы реконнект и копила
/// неподтверждённый WAL слота.
pub const MAX_PRIMARY_KEY_LEN: usize = 255;

/// Строка журнала, подготовленная к вставке в `changes`.
#[derive(Debug, Clone)]
pub struct ChangeRow {
    /// Ключ источника журнала — имя из реестра (ADR-0005): LSN и txid
    /// разных серверов коллидируют, дедуп-ключи журнала включают источник.
    pub source: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub primary_key: Option<String>,
    pub operation: &'static str,
    pub before: Value,
    pub after: Value,
    /// Контекст приложения: payload MESSAGE (ADR-0004). Владение разделяется
    /// между строками одной транзакции (Arc): payload проставляется всем
    /// строкам при коммите, полный клон на каждую строку множил бы память
    /// и CPU на больших транзакциях (тикет #48).
    pub context: Arc<Value>,
    pub committed_at: DateTime<Utc>,
    pub queued_at: DateTime<Utc>,
    pub transaction_id: i64,
    pub position: i64,
    pub event_index: i32,
    /// Идентификатор сквозной операции: копия ключа `trace_id` payload.
    pub trace_id: Option<String>,
}

/// Буфер одной транзакции источника: события копятся между Begin и Commit
/// и вставляются одним батчем в транзакции с чекпоинтом. Буфер ограничен
/// потолком строк (тикет #48): гигантская транзакция клиента не роняет
/// воркер по памяти — строка сверх потолка завершает попытку с ошибкой,
/// транзакция переиграется слотом после реконнекта.
pub struct TransactionBuffer {
    pub xid: i64,
    rows: Vec<ChangeRow>,
    /// Потолок буфера: максимум строк журнала транзакции (включительно).
    max_rows: usize,
    event_index: i32,
    /// Контекст последнего MESSAGE транзакции — его payload (ADR-0004);
    /// разделяется между строками без клонирования.
    context: Arc<Value>,
    /// trace_id последнего MESSAGE с этим ключом: проставляется всем
    /// строкам транзакции при коммите.
    trace_id: Option<String>,
    /// Был ли уже MESSAGE: повторный — коллизия контекста (warn).
    message_seen: bool,
}

impl TransactionBuffer {
    /// Буфер с явным потолком строк; значение ниже 1 поднимается до 1 —
    /// нулевой потолок отвергал бы любую строку. Потолок приходит из
    /// конфигурации воркера (`WorkerConfig::tx_buffer_max_rows`, дефолт —
    /// `DEFAULT_TX_BUFFER_MAX_ROWS`).
    pub fn with_row_limit(xid: u32, max_rows: usize) -> Self {
        Self {
            xid: i64::from(xid),
            rows: Vec::new(),
            max_rows: max_rows.max(1),
            event_index: 0,
            context: Arc::new(json!({})),
            trace_id: None,
            message_seen: false,
        }
    }

    /// Выдаёт порядковый номер записи внутри транзакции источника.
    ///
    /// Нумеруются все записи транзакции — Insert/Update/Delete/Truncate и
    /// MESSAGE, — независимо от игнор-листа: индекс должен быть
    /// детерминированной функцией содержимого транзакции, иначе
    /// replay-дедупликация по ключу `(commit_lsn, event_index)` сломается
    /// на изменении конфигурации.
    pub fn next_index(&mut self) -> i32 {
        let index = self.event_index;
        self.event_index += 1;
        index
    }

    /// Кладёт строку журнала в буфер. Ошибка — потолок буфера превышен:
    /// уже накопленные строки остаются, отвергнутая — нет; диагностика
    /// (warn) пишется здесь, на момент превышения, а попытка целиком
    /// завершается реконнектом с backoff (тикет #48).
    pub fn push_row(&mut self, row: ChangeRow) -> Result<()> {
        if self.rows.len() >= self.max_rows {
            warn!(
                xid = self.xid,
                rows = self.rows.len(),
                limit = self.max_rows,
                "потолок буфера транзакции превышен — транзакция отбрасывается, повтор после реконнекта с чекпоинта"
            );
            return Err(anyhow::anyhow!(
                "потолок буфера транзакции превышен: xid {} уже буферизовал {} строк \
                 при потолке {} — гигантская транзакция клиента отброшена, \
                 после реконнекта переиграется с чекпоинта и упрётся снова",
                self.xid,
                self.rows.len(),
                self.max_rows
            ));
        }
        self.rows.push(row);
        Ok(())
    }

    /// Регистрирует MESSAGE транзакции (ADR-0004): payload становится
    /// контекстом транзакции, а строковое значение ключа `trace_id` —
    /// колонкой всех её строк (извлекается здесь — контракт ключа живёт
    /// в одном месте). Строк журнала MESSAGE не создаёт: единственное
    /// содержимое журнала — изменения строк. Строкам изменений достаётся
    /// контекст последнего MESSAGE: повторный MESSAGE в одной транзакции —
    /// коллизия контекста, warn в лог.
    pub fn push_message(&mut self, context: Value) {
        if self.message_seen {
            warn!(
                xid = self.xid,
                "в транзакции несколько MESSAGE — строкам изменений достанется контекст последнего"
            );
        }
        self.message_seen = true;
        self.trace_id = trace_id_from_payload(&context);
        self.context = Arc::new(context);
    }

    /// Проставляет контекст последнего MESSAGE (payload и `trace_id`)
    /// всем строкам изменений транзакции — включая строки до MESSAGE.
    /// Вызывается при коммите. Владение разделяется (Arc): строки делят
    /// один payload без клонирования. Контекст не выходит за пределы
    /// буфера своей транзакции: строки других транзакций он не затрагивает.
    pub fn apply_transaction_context(&mut self) {
        for row in &mut self.rows {
            row.context = Arc::clone(&self.context);
            row.trace_id = self.trace_id.clone();
        }
    }

    /// Забирает накопленные строки, очищая буфер (счётчик событий
    /// сохраняется).
    pub fn take_rows(&mut self) -> Vec<ChangeRow> {
        std::mem::take(&mut self.rows)
    }

    /// Проставляет время коммита источника всем строкам транзакции —
    /// оно известно только к моменту сообщения Commit.
    pub fn set_committed_at(&mut self, committed_at: DateTime<Utc>) {
        for row in &mut self.rows {
            row.committed_at = committed_at;
        }
    }
}

/// Имя операции в журнале для типа изменения pgoutput.
/// Вставка строки в журнал традиционно называется CREATE.
pub fn operation_name(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Insert => "CREATE",
        ChangeKind::Update => "UPDATE",
        ChangeKind::Delete => "DELETE",
    }
}

/// Таблица в игнор-листе: записи о ней в журнал не попадают.
pub fn is_ignored(ignore_tables: &[String], schema: &str, table: &str) -> bool {
    let qualified = format!("{schema}.{table}");
    ignore_tables.iter().any(|entry| entry == &qualified)
}

/// Срез строки по байтовому пределу с отступом назад до границы символа:
/// многобайтные символы не рвутся. Вызывающий код гарантирует, что строка
/// длиннее предела.
fn truncate_on_char_boundary(value: &str, max_bytes: usize) -> &str {
    debug_assert!(value.len() > max_bytes);
    let mut cutoff = max_bytes;
    while !value.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    &value[..cutoff]
}

/// Обрезает ключ строки под ширину колонки `primary_key` VARCHAR(255).
///
/// Обрезается итоговое закодированное значение: составной ключ,
/// закодированный JSON-массивом, тоже может превысить границу колонки.
/// Срез по границе символов (см. `truncate_on_char_boundary`); ключ в
/// пределах лимита проходит без изменений, отсутствие ключа — не ошибка.
pub fn truncate_primary_key(key: Option<String>) -> Option<String> {
    key.map(|key| {
        if key.len() <= MAX_PRIMARY_KEY_LEN {
            key
        } else {
            warn!(
                original_len = key.len(),
                "primary_key длиннее {MAX_PRIMARY_KEY_LEN} байт — обрезан"
            );
            truncate_on_char_boundary(&key, MAX_PRIMARY_KEY_LEN).to_owned()
        }
    })
}

/// Извлекает `trace_id` верхнего уровня payload MESSAGE (ADR-0004).
///
/// В колонку попадает только строковое значение ключа `trace_id`; сам
/// payload сохраняется в `context` дословно, без вырезания ключа.
/// Отсутствие ключа — не ошибка (колонка остаётся пустой), значение не
/// строки — warn и пустая колонка. Значение длиннее `MAX_TRACE_ID_LEN`
/// обрезается по границе символов: вставка исходной длины уронила бы всю
/// транзакцию вставки.
fn trace_id_from_payload(payload: &Value) -> Option<String> {
    match payload.get(TRACE_ID_KEY) {
        None => None,
        // Значение не строка — warn: клиент нарушает контракт payload,
        // колонка остаётся пустой.
        Some(value) if !value.is_string() => {
            warn!("ключ trace_id в payload MESSAGE не строка — в колонку не попал");
            None
        }
        Some(value) => {
            let trace_id = value.as_str().expect("проверено is_string выше");
            if trace_id.len() <= MAX_TRACE_ID_LEN {
                Some(trace_id.to_owned())
            } else {
                warn!(
                    original_len = trace_id.len(),
                    "trace_id длиннее {MAX_TRACE_ID_LEN} байт — обрезан"
                );
                Some(truncate_on_char_boundary(trace_id, MAX_TRACE_ID_LEN).to_owned())
            }
        }
    }
}

/// Размер чанка вставки: настройка источника, зажатая бюджетом параметров
/// протокола. `batch_size` из реестра может превышать бюджет (правка реестра
/// в обход API, исторические значения) — тогда чанк сжимается до лимита
/// statement, иначе сервер отвергнет вставку и инжест зациклится на
/// реконнекте, копя WAL без ack.
fn chunk_size(batch_size: usize) -> usize {
    batch_size.clamp(1, MAX_ROWS_PER_STATEMENT)
}

/// Чем закончилась вставка батча в журнал.
pub(crate) enum FlushEnd {
    /// Батч и чекпоинт закоммичены.
    Committed,
    /// Остановка пришла во время вставки или между повторами: попытка
    /// прервана досрочно, недосохранённое доиграет replay слота
    /// (дубли гасятся дедупликацией по `(commit_lsn, event_index)`).
    Cancelled,
    /// Вставка не удалась после всех попыток.
    Failed(anyhow::Error),
}

/// Вставляет строки батчами (`push_values`) в одной транзакции с чекпоинтом.
///
/// Дедупликация replay-дублей — `ON CONFLICT DO NOTHING` по уникальному
/// ключу `(commit_lsn, event_index)`. Слот подтверждается только после
/// коммита этой транзакции (вызывающий код делает `update_applied_lsn`).
///
/// Остановка отзывчивая: и сама попытка вставки (даже зажатая блокировкой
/// или зависшей сетью), и пауза между повторами прерываются токеном
/// (`FlushEnd::Cancelled`) — SIGTERM не ждёт ни БД, ни хвоста повторов.
pub async fn flush(
    audit: &PgPool,
    rows: &[ChangeRow],
    slot: &str,
    commit_end_lsn: i64,
    batch_size: usize,
    token: &CancellationToken,
) -> FlushEnd {
    const MAX_ATTEMPTS: u32 = 3;
    let mut attempt = 1;
    loop {
        // Детерминированная проверка до попытки: с уже отменённым токеном
        // к БД не обращаемся вовсе.
        if token.is_cancelled() {
            return FlushEnd::Cancelled;
        }
        let once = tokio::select! {
            _ = token.cancelled() => return FlushEnd::Cancelled,
            once = flush_once(audit, rows, slot, commit_end_lsn, batch_size) => once,
        };
        match once {
            Ok(()) => return FlushEnd::Committed,
            Err(e) if attempt < MAX_ATTEMPTS => {
                warn!(attempt, error = %e, "вставка батча не удалась, повторяем");
                tokio::select! {
                    _ = token.cancelled() => return FlushEnd::Cancelled,
                    _ = tokio::time::sleep(Duration::from_millis(
                        RETRY_BASE_DELAY_MS * u64::from(attempt),
                    )) => {}
                }
                attempt += 1;
            }
            Err(e) => return FlushEnd::Failed(e),
        }
    }
}

async fn flush_once(
    audit: &PgPool,
    rows: &[ChangeRow],
    slot: &str,
    commit_end_lsn: i64,
    batch_size: usize,
) -> Result<()> {
    let mut tx = audit.begin().await?;

    for chunk in rows.chunks(chunk_size(batch_size)) {
        let mut builder = QueryBuilder::new(
            "INSERT INTO changes \
             (source, database, schema, \"table\", primary_key, operation, before, after, context, \
              committed_at, queued_at, transaction_id, position, commit_lsn, event_index, \
              trace_id) ",
        );
        builder.push_values(chunk, |mut b, row| {
            b.push_bind(&row.source)
                .push_bind(&row.database)
                .push_bind(&row.schema)
                .push_bind(&row.table)
                .push_bind(&row.primary_key)
                .push_bind(row.operation)
                .push_bind(&row.before)
                .push_bind(&row.after)
                .push_bind(&*row.context)
                .push_bind(row.committed_at)
                .push_bind(row.queued_at)
                .push_bind(row.transaction_id)
                .push_bind(row.position)
                .push_bind(commit_end_lsn)
                .push_bind(row.event_index)
                .push_bind(&row.trace_id);
        });
        builder.push(" ON CONFLICT DO NOTHING");
        builder.build().execute(&mut *tx).await?;
    }

    sqlx::query(
        "INSERT INTO checkpoints (name, last_commit_lsn, updated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (name) DO UPDATE \
         SET last_commit_lsn = EXCLUDED.last_commit_lsn, updated_at = now()",
    )
    .bind(slot)
    .bind(commit_end_lsn)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_TX_BUFFER_MAX_ROWS;

    /// Строка журнала с заданной операцией для тестов агрегатора.
    fn journal_row_fixture(operation: &'static str) -> ChangeRow {
        ChangeRow {
            source: "client_a".into(),
            database: "db".into(),
            schema: "public".into(),
            table: "t".into(),
            primary_key: None,
            operation,
            before: json!({}),
            after: json!({}),
            context: empty_context(),
            committed_at: Utc::now(),
            queued_at: Utc::now(),
            transaction_id: 1,
            position: 1,
            event_index: 0,
            trace_id: None,
        }
    }

    #[test]
    fn operation_naming_matches_journal_vocabulary() {
        assert_eq!(operation_name(ChangeKind::Insert), "CREATE");
        assert_eq!(operation_name(ChangeKind::Update), "UPDATE");
        assert_eq!(operation_name(ChangeKind::Delete), "DELETE");
    }

    #[test]
    fn ignore_list_matches_qualified_names_only() {
        let list = vec!["public.schema_migrations".to_owned()];
        assert!(is_ignored(&list, "public", "schema_migrations"));
        assert!(!is_ignored(&list, "public", "orders"));
        assert!(!is_ignored(&list, "private", "schema_migrations"));
    }

    #[test]
    fn event_indexes_are_sequential_across_kinds() {
        let mut buffer = TransactionBuffer::with_row_limit(77, DEFAULT_TX_BUFFER_MAX_ROWS);
        assert_eq!(buffer.xid, 77);
        assert_eq!(buffer.next_index(), 0);
        assert_eq!(buffer.next_index(), 1);
        assert!(buffer.take_rows().is_empty());
    }

    #[test]
    fn take_rows_empties_buffer_but_keeps_counter() {
        let mut buffer = TransactionBuffer::with_row_limit(1, DEFAULT_TX_BUFFER_MAX_ROWS);
        buffer.next_index();
        buffer
            .push_row(journal_row_fixture("TRUNCATE"))
            .expect("строка в пределах потолка");
        let rows = buffer.take_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_index, 0);
        assert!(buffer.take_rows().is_empty());
        assert_eq!(buffer.next_index(), 1, "счётчик событий не сбрасывается");
    }

    /// AC (тикет #48): транзакция сверх потолка буфера не роняет процесс —
    /// строка сверх потолка отвергается с явной ошибкой, уже накопленные
    /// строки остаются в буфере; попытка целиком завершится реконнектом
    /// с backoff, транзакция переиграется слотом с чекпоинта.
    #[test]
    fn push_row_over_limit_fails_without_buffering() {
        let mut buffer = TransactionBuffer::with_row_limit(9, 2);

        buffer
            .push_row(journal_row_fixture("CREATE"))
            .expect("первая строка в пределах потолка");
        buffer
            .push_row(journal_row_fixture("UPDATE"))
            .expect("вторая строка в пределах потолка");

        let error = buffer
            .push_row(journal_row_fixture("DELETE"))
            .expect_err("строка сверх потолка отвергается с ошибкой");
        assert!(
            error.to_string().contains("потолок"),
            "диагностика называет потолок буфера: {error}"
        );
        assert_eq!(
            buffer.take_rows().len(),
            2,
            "отвергнутая строка не попадает в буфер"
        );
    }

    /// Потолок — максимум строк в буфере, включительно: транзакция ровно
    /// в потолок проходит целиком; потолок ниже 1 поднимается до 1.
    #[test]
    fn row_limit_is_inclusive_and_clamped() {
        let mut buffer = TransactionBuffer::with_row_limit(1, 1);
        buffer
            .push_row(journal_row_fixture("CREATE"))
            .expect("одна строка — ровно потолок");
        assert!(
            buffer.push_row(journal_row_fixture("CREATE")).is_err(),
            "вторая строка сверх потолка"
        );

        let mut buffer = TransactionBuffer::with_row_limit(1, 0);
        buffer
            .push_row(journal_row_fixture("CREATE"))
            .expect("нулевой потолок поднят до 1");
        assert!(buffer.push_row(journal_row_fixture("CREATE")).is_err());
    }

    #[test]
    fn trace_id_is_extracted_from_top_level_string() {
        let payload = json!({"trace_id": "op-1", "user": "alice"});
        assert_eq!(trace_id_from_payload(&payload).as_deref(), Some("op-1"));
        // Payload не мутируется: context хранит его дословно (ADR-0004).
        assert_eq!(payload["trace_id"], "op-1");
        assert_eq!(payload["user"], "alice");
    }

    #[test]
    fn trace_id_requires_top_level_string_key() {
        assert_eq!(trace_id_from_payload(&json!({"user": "bob"})), None);
        assert_eq!(trace_id_from_payload(&json!({})), None);
        // Нестроковые значения в колонку не попадают.
        assert_eq!(trace_id_from_payload(&json!({"trace_id": 42})), None);
        assert_eq!(trace_id_from_payload(&json!({"trace_id": ["op"]})), None);
    }

    #[test]
    fn trace_id_is_truncated_to_column_width() {
        let long = "x".repeat(MAX_TRACE_ID_LEN + 10);
        let extracted = trace_id_from_payload(&json!({ "trace_id": long }));
        assert_eq!(extracted.expect("строка").len(), MAX_TRACE_ID_LEN);
    }

    #[test]
    fn rows_per_statement_fits_protocol_parameter_budget() {
        // Бюджет параметров протокола: 65535 на statement, 16 на строку
        // журнала → максимум 4095 строк; инвариант полного чанка —
        // compile-тайм проверка рядом с константами.
        assert_eq!(MAX_STATEMENT_PARAMS, 65_535);
        assert_eq!(CHANGE_ROW_PARAMS, 16);
        assert_eq!(CHANGE_ROW_PARAMS * MAX_ROWS_PER_STATEMENT, 65_520);
        assert_eq!(MAX_ROWS_PER_STATEMENT, 4_095);
    }

    #[test]
    fn chunk_size_clamps_batch_to_statement_budget() {
        assert_eq!(chunk_size(0), 1, "нулевой batch_size не ломает вставку");
        assert_eq!(chunk_size(1), 1);
        assert_eq!(chunk_size(500), 500, "обычная настройка не сжимается");
        assert_eq!(
            chunk_size(MAX_ROWS_PER_STATEMENT),
            MAX_ROWS_PER_STATEMENT,
            "верхняя граница бюджета исполняется одним statement"
        );
        assert_eq!(
            chunk_size(10_000),
            MAX_ROWS_PER_STATEMENT,
            "batch_size выше бюджета (историческая граница API) сжимается"
        );
    }

    #[test]
    fn budget_chunking_covers_every_row_without_oversize_chunks() {
        // Тест чанкования: 10 000 строк (историческая верхняя граница API)
        // нарезаются на чанки в пределах бюджета, без потерь и дублей.
        let rows = vec![journal_row_fixture("CREATE"); 10_000];
        let lengths: Vec<usize> = rows
            .chunks(chunk_size(10_000))
            .map(<[ChangeRow]>::len)
            .collect();

        assert!(lengths.iter().all(|len| *len <= MAX_ROWS_PER_STATEMENT));
        assert_eq!(
            lengths.iter().sum::<usize>(),
            rows.len(),
            "строки не теряются"
        );
        assert_eq!(
            lengths,
            vec![
                MAX_ROWS_PER_STATEMENT,
                MAX_ROWS_PER_STATEMENT,
                10_000 - 2 * MAX_ROWS_PER_STATEMENT,
            ],
            "нарезка: полные чанки по бюджету + хвост"
        );

        // Настройка источника меньше бюджета тоже соблюдается.
        let lengths: Vec<usize> = rows
            .chunks(chunk_size(500))
            .map(<[ChangeRow]>::len)
            .collect();
        assert_eq!(lengths, vec![500; 20]);
    }

    #[test]
    fn multibyte_trace_id_is_truncated_on_char_boundary() {
        // «ж» — 2 байта: байтовый предел 255 попадает в середину символа.
        let long = "ж".repeat(130);
        let extracted = trace_id_from_payload(&json!({ "trace_id": long })).expect("строка");
        assert!(extracted.len() <= MAX_TRACE_ID_LEN);
        assert!(extracted.chars().all(|c| c == 'ж'));
    }

    #[test]
    fn primary_key_is_truncated_to_column_width() {
        let long = "k".repeat(MAX_PRIMARY_KEY_LEN + 10);
        let truncated = truncate_primary_key(Some(long)).expect("строка");
        assert_eq!(truncated.len(), MAX_PRIMARY_KEY_LEN);
    }

    #[test]
    fn multibyte_primary_key_is_truncated_on_char_boundary() {
        // «ключ-» — 9 байт: байтовый предел 255 попадает в середину символа.
        let long = "ключ-".repeat(60);
        let truncated = truncate_primary_key(Some(long)).expect("строка");
        assert!(truncated.len() <= MAX_PRIMARY_KEY_LEN);
        assert!(truncated.chars().all(|c| "ключ-".contains(c)));
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn composite_key_encoded_as_json_is_truncated_as_whole_value() {
        // Составной ключ кодируется JSON-массивом строк (pgoutput:
        // key_string): компоненты по отдельности короткие, итоговое
        // закодированное значение — длиннее предела. Обрезается значение
        // целиком: закрывающая кавычка и скобка JSON не сохраняются.
        let encoded = json!(["a".repeat(200), "b".repeat(200)]).to_string();
        assert!(encoded.len() > MAX_PRIMARY_KEY_LEN);
        let truncated = truncate_primary_key(Some(encoded)).expect("строка");
        let a = "a".repeat(200);
        let b = "b".repeat(50);
        assert_eq!(truncated, format!("[\"{a}\",\"{b}"));
    }

    #[test]
    fn primary_key_within_limit_passes_through_unchanged() {
        assert_eq!(truncate_primary_key(None), None);
        assert_eq!(truncate_primary_key(Some("7".into())).as_deref(), Some("7"));
        let exact = "x".repeat(MAX_PRIMARY_KEY_LEN);
        assert_eq!(
            truncate_primary_key(Some(exact.clone())).as_deref(),
            Some(exact.as_str())
        );
    }

    /// Владение контекстом разделяется (Arc): строки одной транзакции
    /// делят один payload без полного клона на строку (тикет #48).
    #[test]
    fn transaction_context_is_shared_between_rows_not_cloned() {
        let mut buffer = TransactionBuffer::with_row_limit(1, DEFAULT_TX_BUFFER_MAX_ROWS);
        buffer
            .push_row(journal_row_fixture("CREATE"))
            .expect("строка в пределах потолка");
        buffer
            .push_row(journal_row_fixture("UPDATE"))
            .expect("строка в пределах потолка");
        buffer.push_message(json!({"trace_id": "op-1"}));

        buffer.apply_transaction_context();
        let rows = buffer.take_rows();
        assert_eq!(rows.len(), 2);
        assert!(
            Arc::ptr_eq(&rows[0].context, &rows[1].context),
            "строки транзакции делят один контекст без клонирования"
        );
    }

    #[test]
    fn message_creates_no_journal_row_and_enriches_change_rows() {
        let mut buffer = TransactionBuffer::with_row_limit(7, DEFAULT_TX_BUFFER_MAX_ROWS);
        buffer
            .push_row(journal_row_fixture("CREATE"))
            .expect("строка в пределах потолка");
        buffer.push_message(json!({"trace_id": "op-1"}));
        buffer
            .push_row(journal_row_fixture("UPDATE"))
            .expect("строка в пределах потолка");

        buffer.apply_transaction_context();
        let rows = buffer.take_rows();
        assert_eq!(rows.len(), 2, "MESSAGE строк журнала не создаёт (ADR-0004)");
        // Обе строки изменения: контекст MESSAGE и trace_id в колонке,
        // включая строку до MESSAGE.
        for row in &rows {
            assert_eq!(*row.context, json!({"trace_id": "op-1"}));
            assert_eq!(row.trace_id.as_deref(), Some("op-1"));
        }
    }

    #[test]
    fn last_message_wins_for_change_rows() {
        let mut buffer = TransactionBuffer::with_row_limit(7, DEFAULT_TX_BUFFER_MAX_ROWS);
        buffer
            .push_row(journal_row_fixture("CREATE"))
            .expect("строка в пределах потолка");
        buffer.push_message(json!({"trace_id": "op-1"}));
        buffer.push_message(json!({"trace_id": "op-2", "user": "bob"}));

        buffer.apply_transaction_context();
        let rows = buffer.take_rows();
        assert_eq!(rows.len(), 1, "повторный MESSAGE строк не добавляет");
        // Строка изменения: контекст последнего MESSAGE, даже если она
        // была до него.
        assert_eq!(*rows[0].context, json!({"trace_id": "op-2", "user": "bob"}));
        assert_eq!(rows[0].trace_id.as_deref(), Some("op-2"));
    }

    #[test]
    fn message_without_trace_id_leaves_column_empty() {
        let mut buffer = TransactionBuffer::with_row_limit(1, DEFAULT_TX_BUFFER_MAX_ROWS);
        buffer
            .push_row(journal_row_fixture("UPDATE"))
            .expect("строка в пределах потолка");
        buffer.push_message(json!({"user": "bob"}));

        buffer.apply_transaction_context();
        let rows = buffer.take_rows();
        assert_eq!(rows[0].trace_id, None, "колонка пустая без ключа");
        assert_eq!(*rows[0].context, json!({"user": "bob"}));
    }

    #[test]
    fn message_only_transaction_creates_no_rows() {
        let mut buffer = TransactionBuffer::with_row_limit(1, DEFAULT_TX_BUFFER_MAX_ROWS);
        // MESSAGE расходует индекс события, но строку журнала не создаёт:
        // индекс остаётся детерминированной функцией содержимого транзакции.
        buffer.next_index();
        buffer.push_message(json!({"trace_id": "op-1"}));
        buffer.apply_transaction_context();
        assert!(
            buffer.take_rows().is_empty(),
            "транзакция только с MESSAGE не создаёт строк журнала (ADR-0004)"
        );
    }

    #[test]
    fn transaction_context_does_not_leak_into_next_buffer() {
        let mut buffer = TransactionBuffer::with_row_limit(1, DEFAULT_TX_BUFFER_MAX_ROWS);
        buffer.push_message(json!({"trace_id": "op-1"}));
        buffer.apply_transaction_context();

        // Новая транзакция без MESSAGE — строки с пустым контекстом.
        let mut next = TransactionBuffer::with_row_limit(2, DEFAULT_TX_BUFFER_MAX_ROWS);
        next.push_row(journal_row_fixture("DELETE"))
            .expect("строка в пределах потолка");
        next.apply_transaction_context();
        let rows = next.take_rows();
        assert_eq!(*rows[0].context, json!({}));
        assert_eq!(rows[0].trace_id, None);
    }

    #[test]
    fn change_rows_without_messages_keep_empty_context() {
        let mut buffer = TransactionBuffer::with_row_limit(1, DEFAULT_TX_BUFFER_MAX_ROWS);
        buffer
            .push_row(journal_row_fixture("UPDATE"))
            .expect("строка в пределах потолка");
        buffer.apply_transaction_context();
        let rows = buffer.take_rows();
        assert_eq!(*rows[0].context, json!({}));
        assert_eq!(rows[0].trace_id, None);
    }

    /// Отмена до первой попытки: с уже отменённым токеном цикл вставки
    /// не обращается к БД — пул ленивый и указывает на недоступный адрес,
    /// любая попытка упёрлась бы в acquire_timeout. Остановка не ждёт
    /// ни попыток, ни пауз повторов (тикет #46).
    #[tokio::test]
    async fn flush_with_cancelled_token_aborts_before_any_attempt() {
        let token = CancellationToken::new();
        token.cancel();
        let audit = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://invalid:invalid@127.0.0.1:1/none")
            .expect("ленивый пул создаётся без подключения");

        let started = std::time::Instant::now();
        let end = flush(&audit, &[], "слот-отмены", 1, 10, &token).await;

        assert!(matches!(end, FlushEnd::Cancelled));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "отменённый flush не должен обращаться к БД: {:?}",
            started.elapsed()
        );
    }
}
