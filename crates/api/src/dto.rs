use std::collections::HashMap;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use sqlx::prelude::*;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

// --- Границы пагинации: без магических чисел ---

/// Минимальный номер страницы.
pub const MIN_PAGE: i32 = 1;
/// Минимальный размер страницы.
pub const MIN_PER_PAGE: i32 = 1;
/// Максимальный размер страницы.
pub const MAX_PER_PAGE: i32 = 200;

/// Строка журнала в списке: изменение или событие приложения (ADR-0002).
///
/// `changes` — краткая сводка образов «до/после» по первым четырём ключам;
/// для MESSAGE-строк — пустой массив: у события образов нет, payload целиком
/// в `context`.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ChangeRowDTO {
    pub id: Uuid,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub operation: String,
    pub primary_key: Option<String>,
    /// Идентификатор сквозной операции: объединяет изменения и события одной операции.
    pub trace_id: Option<String>,
    #[schema(
        value_type = Vec<serde_json::Value>,
        example = "[{\"key\": \"name\", \"before\": \"John\", \"after\": \"Jane\"}]",
    )]
    pub changes: serde_json::Value,
    #[schema(value_type = serde_json::Value)]
    pub context: serde_json::Value,
    /// Когда запись была изменена в источнике; NULL — строки старого пайплайна.
    pub committed_at: Option<DateTime<chrono::Utc>>,
    /// Когда запись сохранена в журнал.
    pub created_at: DateTime<chrono::Utc>,
}

/// Страница журнала.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChangePageDTO {
    pub page: i32,
    pub pages: i64,
    pub rows: Vec<ChangeRowDTO>,
}

/// Параметры списка журнала.
///
/// Незнакомые параметры запроса трактуются как фильтры вхождения в
/// `context` (GIN `@>`): точка в имени параметра задаёт вложенность,
/// например `?user.login=alice` → `context @> {"user": {"login": "alice"}}`.
#[derive(Deserialize, Debug, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QChange {
    /// Номер страницы, с единицы.
    #[param(minimum = 1)]
    #[serde(default = "default_page")]
    pub page: i32,

    /// Размер страницы.
    #[param(minimum = 1, maximum = 200)]
    #[serde(default = "default_per_page")]
    pub per_page: i32,

    /// Идентификатор сквозной операции: вся операция целиком — изменения и события.
    pub trace_id: Option<String>,

    /// Имя таблицы, без учёта регистра.
    pub table: Option<String>,

    /// Операция: CREATE, UPDATE, DELETE, TRUNCATE или MESSAGE, без учёта регистра.
    pub operation: Option<String>,

    /// Диапазон времени сохранения в журнал: `2025-01-01..2025-12-31`;
    /// одна граница — нижняя: `2025-01-01`.
    #[param(example = "2025-01-01..2025-12-31")]
    pub created_at: Option<String>,

    /// Направление сортировки по времени сохранения: `created_at:asc|desc`.
    #[param(example = "created_at:desc")]
    #[serde(default = "default_sortable")]
    pub sortable: String,

    /// Остальные параметры — фильтры вхождения в `context`.
    #[serde(flatten)]
    pub context_filters: HashMap<String, String>,
}

impl QChange {
    /// Номер страницы, нормализованный к нижней границе.
    pub fn normalized_page(&self) -> i32 {
        self.page.max(MIN_PAGE)
    }

    /// Размер страницы, нормализованный к допустимому диапазону.
    pub fn normalized_per_page(&self) -> i32 {
        self.per_page.clamp(MIN_PER_PAGE, MAX_PER_PAGE)
    }
}

/// Детальная строка журнала: полные образы «до/после» и контекст.
///
/// Значения ключа `password` в образах маскируются.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ChangeDetailDTO {
    pub id: Uuid,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub operation: String,
    pub primary_key: Option<String>,
    pub trace_id: Option<String>,
    #[schema(value_type = serde_json::Value)]
    #[serde(serialize_with = "mask_sensitive_data")]
    pub before: serde_json::Value,
    #[schema(value_type = serde_json::Value)]
    #[serde(serialize_with = "mask_sensitive_data")]
    pub after: serde_json::Value,
    #[schema(value_type = serde_json::Value)]
    pub context: serde_json::Value,
    pub committed_at: Option<DateTime<chrono::Utc>>,
    pub created_at: DateTime<chrono::Utc>,
}

/// Маскирует чувствительные ключи верхнего уровня образа.
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

/// Значение номера страницы по умолчанию.
fn default_page() -> i32 {
    1
}

/// Значение размера страницы по умолчанию.
fn default_per_page() -> i32 {
    25
}

/// Значение сортировки по умолчанию: свежие сверху.
fn default_sortable() -> String {
    "created_at:desc".to_string()
}
