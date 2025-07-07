use std::collections::HashMap;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use sqlx::prelude::*;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct AuditDTO {
    pub id: Uuid,
    pub status_code: i16,
    pub event: String,
    pub primary_key: Option<String>,
    pub is_change: bool,
    #[serde(flatten)]
    pub context: serde_json::Value,
    pub created_at: DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuditPaginateDTO {
    pub page: i32,
    pub pages: i64,
    pub rows: Vec<AuditDTO>,
}

#[derive(Deserialize, Debug, Validate, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QAudit {
    #[validate(range(min = 1))]
    #[serde(default = "default_page")]
    pub page: i32,

    #[validate(range(min = 1))]
    #[serde(default = "default_per_page")]
    pub per_page: i32,
    // fields
    pub event: Option<String>,
    pub primary_key: Option<String>,
    pub status_code: Option<i16>,
    #[param(example = "2025-01-01 || ..2025-12-31 || 2025-01-01..2025-12-31")]
    pub created_at: Option<String>,
    // Context в JSONB
    pub app: Option<String>,
    pub ip: Option<String>,
    pub method: Option<String>,
    pub user_login: Option<String>,
    pub user_fio: Option<String>,
    pub user_role_name: Option<String>,
    #[param(example = "created_at:desc")]
    #[serde(default = "default_sortable")]
    pub sortable: String,
}

/// Функция значения по умолчанию
fn default_sortable() -> String {
    "created_at:desc".to_string()
}

/// Функция значения по умолчанию
fn default_per_page() -> i32 {
    25
}

/// Функция значения по умолчанию
fn default_page() -> i32 {
    1
}

// Changes

#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ChangeDTO {
    pub id: Uuid,
    pub database: String,
    pub operation: String,
    pub table: String,
    #[schema(
        value_type = Vec<serde_json::Value>,
        example = "[{\"key\": \"name\", \"before\": \"John\", \"after\": \"Jane\"}]",
    )]
    pub changes: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ChangeDetailDTO {
    pub id: Uuid,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub operation: String,
    pub primary_key: Option<String>,
    #[schema(value_type = serde_json::Value)]
    #[serde(serialize_with = "mask_sensitive_data")]
    pub before: serde_json::Value,
    #[schema(value_type = serde_json::Value)]
    #[serde(serialize_with = "mask_sensitive_data")]
    pub after: serde_json::Value,
    pub context: serde_json::Value,
}

/// Функция для обратотки ключей и скрытие маской
fn mask_sensitive_data<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut masked_value = value.clone();

    // Список чувствительных ключей
    let sensitive_keys = ["password"];

    if let serde_json::Value::Object(obj) = &mut masked_value {
        for key in obj.keys().cloned().collect::<Vec<String>>() {
            if sensitive_keys.contains(&key.as_str()) {
                obj.insert(key, serde_json::Value::String("*****".to_string()));
            }
        }
    }

    masked_value.serialize(serializer)
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ChangePaginateDTO {
    pub offset: i32,
    pub cnt: i64,
    pub rows: Vec<ChangeDTO>,
}

#[derive(Deserialize, Debug, Validate, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QChange {
    #[validate(range(min = 0))]
    #[serde(default = "default_offset")]
    pub offset: i32,

    #[validate(range(min = 1))]
    #[serde(default = "default_limit")]
    pub limit: i32,

    pub table: Option<String>,
    pub operation: Option<String>,

    #[serde(flatten, skip_serializing)]
    pub filters: HashMap<String, String>,
}

/// Функция значения по умолчанию
pub fn default_limit() -> i32 {
    15
}

/// Функция значения по умолчанию
pub fn default_offset() -> i32 {
    0
}
