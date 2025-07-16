use crate::Result;
use serde::{Deserialize, Serialize};
use sqlx::{
    Postgres, Transaction,
    types::chrono::{DateTime, Utc},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChangeDTO {
    pub database: String,            // sourse.db
    pub schema: String,              // sourse.schema
    pub table: String,               // sourse.table
    pub primary_key: Option<String>, // before.id, after.id
    pub operation: String,           // op
    pub before: serde_json::Value,
    pub after: serde_json::Value,
    pub context: serde_json::Value,
    pub request_id: Option<String>,  // context.request_id
    pub committed_at: DateTime<Utc>, // sourse.ts_ms
    pub queued_at: DateTime<Utc>,    // ts_ms
    pub created_at: DateTime<Utc>,   // NOW()
    pub transaction_id: i64,         // sourse.txId
    pub position: i64,               // sourse.lsn
}

impl ChangeDTO {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: String,
        schema: String,
        table: String,
        primary_key: Option<String>,
        operation: String,
        before: serde_json::Value,
        after: serde_json::Value,
        context: serde_json::Value,
        request_id: Option<String>,
        committed_at: DateTime<Utc>,
        queued_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
        transaction_id: i64,
        position: i64,
    ) -> Self {
        Self {
            database,
            schema,
            table,
            primary_key,
            operation,
            before,
            after,
            context,
            request_id,
            committed_at,
            queued_at,
            created_at,
            transaction_id,
            position,
        }
    }
}

pub(crate) async fn insert_rows_change(
    tx: &mut Transaction<'_, Postgres>,
    changes: &[ChangeDTO],
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }

    let dt = Utc::now();

    // Формируем запрос с множественными значениями
    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO changes (database, schema, \"table\", primary_key, operation, before, after, context, request_id, committed_at, queued_at, transaction_id, position, created_at) ",
    );

    query_builder.push_values(changes, |mut b, change| {
        b.push_bind(&change.database)
            .push_bind(&change.schema)
            .push_bind(&change.table)
            .push_bind(&change.primary_key)
            .push_bind(&change.operation)
            .push_bind(&change.before)
            .push_bind(&change.after)
            .push_bind(&change.context)
            .push_bind(&change.request_id)
            .push_bind(change.committed_at)
            .push_bind(change.queued_at)
            .push_bind(change.transaction_id)
            .push_bind(change.position)
            .push_bind(dt);
    });

    query_builder.push(" ON CONFLICT DO NOTHING");

    let query = query_builder.build();
    query.execute(&mut **tx).await?;

    Ok(())
}
