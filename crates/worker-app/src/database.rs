use crate::Result;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction, types::chrono::Utc};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct AuditDTO {
    pub id: Uuid,
    pub event: String,
    pub primary_key: Option<String>,
    pub status_code: i16,
    pub response_body: String,
    pub context: serde_json::Value,
}

impl AuditDTO {
    pub fn new(
        id: Uuid,
        event: String,
        primary_key: Option<String>,
        status_code: i16,
        response_body: String,
        context: serde_json::Value,
    ) -> Self {
        Self {
            id,
            event,
            primary_key,
            status_code,
            response_body,
            context,
        }
    }
}

pub(crate) async fn insert_rows_audit(
    tx: &mut Transaction<'_, Postgres>,
    records: &[AuditDTO],
) -> Result<()> {
    let dt = Utc::now();

    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO audits (id, event, primary_key, status_code, response_body, context, created_at) ",
    );

    query_builder.push_values(records, |mut b, row| {
        b.push_bind(row.id)
            .push_bind(&row.event)
            .push_bind(&row.primary_key)
            .push_bind(row.status_code)
            .push_bind(&row.response_body)
            .push_bind(&row.context)
            .push_bind(dt);
    });

    query_builder.push(" ON CONFLICT DO NOTHING");

    let query = query_builder.build();
    query.execute(&mut **tx).await?;

    Ok(())
}
