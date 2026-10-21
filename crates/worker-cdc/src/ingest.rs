use chrono::{DateTime, Utc};
use pgoutput::ChangeKind;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::QueryBuilder;
use std::time::Duration;
use tracing::warn;

use crate::Result;

/// Строка журнала, подготовленная к вставке в `changes`.
#[derive(Debug, Clone)]
pub struct ChangeRow {
    pub database: String,
    pub schema: String,
    pub table: String,
    pub primary_key: Option<String>,
    pub operation: &'static str,
    pub before: Value,
    pub after: Value,
    pub committed_at: DateTime<Utc>,
    pub queued_at: DateTime<Utc>,
    pub transaction_id: i64,
    pub position: i64,
    pub event_index: i32,
}

/// Буфер одной транзакции источника: события копятся между Begin и Commit
/// и вставляются одним батчем в транзакции с чекпоинтом.
pub struct TransactionBuffer {
    pub xid: i64,
    rows: Vec<ChangeRow>,
    event_index: i32,
}

impl TransactionBuffer {
    pub fn new(xid: u32) -> Self {
        Self {
            xid: i64::from(xid),
            rows: Vec::new(),
            event_index: 0,
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

    pub fn push_row(&mut self, row: ChangeRow) {
        self.rows.push(row);
    }

    pub fn rows(&self) -> &[ChangeRow] {
        &self.rows
    }

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
/// Вставка в журнал называется CREATE — так было в Debezium-пайплайне.
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

/// Вставляет строки батчами (`push_values`) в одной транзакции с чекпоинтом.
///
/// Дедупликация replay-дублей — `ON CONFLICT DO NOTHING` по уникальному
/// ключу `(commit_lsn, event_index)`. Слот подтверждается только после
/// коммита этой транзакции (вызывающий код делает `update_applied_lsn`).
pub async fn flush(
    audit: &PgPool,
    rows: &[ChangeRow],
    slot: &str,
    commit_end_lsn: i64,
    batch_size: usize,
) -> Result<()> {
    const MAX_ATTEMPTS: u32 = 3;
    let mut attempt = 1;
    loop {
        match flush_once(audit, rows, slot, commit_end_lsn, batch_size).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_ATTEMPTS => {
                warn!(attempt, error = %e, "вставка батча не удалась, повторяем");
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
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

    for chunk in rows.chunks(batch_size.max(1)) {
        let mut builder = QueryBuilder::new(
            "INSERT INTO changes \
             (database, schema, \"table\", primary_key, operation, before, after, context, \
              committed_at, queued_at, transaction_id, position, commit_lsn, event_index) ",
        );
        builder.push_values(chunk, |mut b, row| {
            b.push_bind(&row.database)
                .push_bind(&row.schema)
                .push_bind(&row.table)
                .push_bind(&row.primary_key)
                .push_bind(row.operation)
                .push_bind(&row.before)
                .push_bind(&row.after)
                // Контекст из MESSAGE подставляется тикетом #8 (ADR-0002).
                .push_bind(json!({}))
                .push_bind(row.committed_at)
                .push_bind(row.queued_at)
                .push_bind(row.transaction_id)
                .push_bind(row.position)
                .push_bind(commit_end_lsn)
                .push_bind(row.event_index);
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
        let mut buffer = TransactionBuffer::new(77);
        assert_eq!(buffer.xid, 77);
        assert_eq!(buffer.next_index(), 0);
        assert_eq!(buffer.next_index(), 1);
        assert!(buffer.rows().is_empty());
    }

    #[test]
    fn take_rows_empties_buffer_but_keeps_counter() {
        let mut buffer = TransactionBuffer::new(1);
        buffer.next_index();
        buffer.push_row(ChangeRow {
            database: "db".into(),
            schema: "public".into(),
            table: "t".into(),
            primary_key: None,
            operation: "TRUNCATE",
            before: json!({}),
            after: json!({}),
            committed_at: Utc::now(),
            queued_at: Utc::now(),
            transaction_id: 1,
            position: 42,
            event_index: 0,
        });
        assert_eq!(buffer.rows().len(), 1);
        let rows = buffer.take_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_index, 0);
        assert!(buffer.rows().is_empty());
        assert_eq!(buffer.next_index(), 1, "счётчик событий не сбрасывается");
    }
}
