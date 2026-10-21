use std::collections::HashMap;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use sqlx::prelude::*;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;
use validator::{Validate, ValidationError};

use crate::{AppError, AppResult};

/// Диапазон времени фильтра: (нижняя граница, верхняя граница).
pub(crate) type TimeRange = (Option<DateTime<chrono::Utc>>, Option<DateTime<chrono::Utc>>);

/// Строка журнала: изменение одной строки источника (ADR-0004).
///
/// `changes` — краткая сводка образов «до/после» по первым четырём ключам.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ChangeRowDTO {
    pub id: Uuid,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub operation: String,
    pub primary_key: Option<String>,
    /// Идентификатор сквозной операции: объединяет изменения строк одной операции.
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
/// Всё, что приходит извне, проверяется на уровне axum: экстрактор
/// `Valid<Query<_>>` возвращает 400 при любом нарушении (диапазоны,
/// формат времени и сортировки). Незнакомые параметры запроса трактуются
/// как фильтры вхождения в `context` (GIN `@>`): точка в имени параметра
/// задаёт вложенность, например `?user.login=alice` →
/// `context @> {"user": {"login": "alice"}}`.
#[derive(Deserialize, Debug, Validate, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct QChange {
    /// Номер страницы, с единицы.
    #[param(minimum = 1)]
    #[validate(range(min = 1))]
    #[serde(default = "default_page")]
    pub page: i32,

    /// Размер страницы.
    #[param(minimum = 1, maximum = 200)]
    #[validate(range(min = 1, max = 200))]
    #[serde(default = "default_per_page")]
    pub per_page: i32,

    /// Идентификатор сквозной операции: вся операция целиком — все изменения строк транзакции.
    pub trace_id: Option<String>,

    /// Имя таблицы, без учёта регистра.
    pub table: Option<String>,

    /// Операция: CREATE, UPDATE, DELETE или TRUNCATE, без учёта регистра.
    pub operation: Option<String>,

    /// Диапазон времени сохранения в журнал: `2025-01-01..2025-12-31`;
    /// одна граница — нижняя: `2025-01-01`. Границы — RFC 3339.
    #[param(example = "2025-01-01..2025-12-31")]
    #[validate(custom(function = "validate_time_range"))]
    pub created_at: Option<String>,

    /// Направление сортировки по времени сохранения: `created_at:asc|desc`.
    #[param(example = "created_at:desc")]
    #[validate(custom(function = "validate_sortable"))]
    #[serde(default = "default_sortable")]
    pub sortable: String,

    /// Остальные параметры — фильтры вхождения в `context`.
    #[serde(flatten)]
    pub context_filters: HashMap<String, String>,
}

impl QChange {
    /// Диапазон времени фильтра в типизированном виде: формат уже проверен
    /// на входе (axum-valid), поэтому ошибка парсинга здесь недостижима и
    /// трактуется как ошибка сервера, а не молчаливое игнорирование.
    pub fn time_range(&self) -> AppResult<TimeRange> {
        let Some(val) = &self.created_at else {
            return Ok((None, None));
        };

        let (start_raw, end_raw) = match val.split_once("..") {
            Some((start, end)) => (Some(start), Some(end)),
            None => (Some(val.as_str()), None),
        };

        let start = match start_raw {
            Some(raw) => Some(parse_boundary(raw)?),
            None => None,
        };
        let end = match end_raw {
            Some(raw) => Some(parse_boundary(raw)?),
            None => None,
        };

        Ok((start, end))
    }

    /// Направление сортировки: `true` — от свежих к старым (по умолчанию).
    pub fn descending(&self) -> bool {
        match self.sortable.split_once(':') {
            Some((_, direction)) => !direction.eq_ignore_ascii_case("asc"),
            None => true,
        }
    }
}

/// Разбирает границу диапазона времени.
fn parse_boundary(raw: &str) -> AppResult<DateTime<chrono::Utc>> {
    raw.parse::<DateTime<chrono::Utc>>()
        .map_err(|e| AppError::bad_request(format!("Некорректная дата-время `{raw}`: {e}")))
}

/// Проверка параметра времени: `start..end` или одна нижняя граница,
/// каждая часть — RFC 3339.
fn validate_time_range(val: &str) -> Result<(), ValidationError> {
    let parts = match val.split_once("..") {
        Some((start, end)) => vec![start, end],
        None => vec![val],
    };

    for part in parts {
        if part.parse::<DateTime<chrono::Utc>>().is_err() {
            return Err(ValidationError::new(
                "границы — RFC 3339, например 2025-01-01T00:00:00Z",
            ));
        }
    }

    Ok(())
}

/// Проверка параметра сортировки: только `created_at:asc` или
/// `created_at:desc`, без учёта регистра.
fn validate_sortable(val: &str) -> Result<(), ValidationError> {
    let valid = matches!(
        val.split_once(':'),
        Some((field, direction))
            if field.eq_ignore_ascii_case("created_at")
                && (direction.eq_ignore_ascii_case("asc")
                    || direction.eq_ignore_ascii_case("desc"))
    );

    if valid {
        Ok(())
    } else {
        Err(ValidationError::new(
            "допустимо created_at:asc или created_at:desc",
        ))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn query(sortable: &str, created_at: Option<&str>) -> QChange {
        QChange {
            page: 1,
            per_page: 25,
            trace_id: None,
            table: None,
            operation: None,
            created_at: created_at.map(str::to_string),
            sortable: sortable.to_string(),
            context_filters: HashMap::new(),
        }
    }

    #[test]
    fn sortable_accepts_created_at_directions_only() {
        assert!(validate_sortable("created_at:asc").is_ok());
        assert!(validate_sortable("created_at:desc").is_ok());
        assert!(validate_sortable("CREATED_AT:DESC").is_ok());
        assert!(validate_sortable("created_at:DESC").is_ok());
        assert!(validate_sortable("committed_at:asc").is_err());
        assert!(validate_sortable("created_at:sideways").is_err());
        assert!(validate_sortable("created_at").is_err());
        assert!(validate_sortable("").is_err());
    }

    #[test]
    fn time_range_validates_rfc3339_boundaries() {
        assert!(validate_time_range("2025-01-01T00:00:00Z").is_ok());
        assert!(validate_time_range("2025-01-01T00:00:00Z..2025-12-31T23:59:59Z").is_ok());
        assert!(validate_time_range("not-a-date").is_err());
        assert!(validate_time_range("2025-01-01T00:00:00Z..wrong").is_err());
        assert!(validate_time_range("2025-01-01T00:00:00Z..").is_err());
        assert!(validate_time_range("").is_err());
    }

    #[test]
    fn time_range_parses_single_bound_as_lower() {
        let (start, end) = query("created_at:desc", Some("2025-01-01T00:00:00Z"))
            .time_range()
            .expect("парсинг");
        assert_eq!(
            start.map(|dt| dt.to_rfc3339()),
            Some("2025-01-01T00:00:00+00:00".to_string())
        );
        assert_eq!(end, None);
    }

    #[test]
    fn time_range_parses_both_bounds() {
        let (start, end) = query(
            "created_at:asc",
            Some("2025-01-01T00:00:00Z..2025-02-01T12:30:00Z"),
        )
        .time_range()
        .expect("парсинг");
        assert!(start.is_some());
        assert_eq!(
            end.map(|dt| dt.to_rfc3339()),
            Some("2025-02-01T12:30:00+00:00".to_string())
        );
    }

    #[test]
    fn no_created_at_gives_no_bounds() {
        let (start, end) = query("created_at:desc", None)
            .time_range()
            .expect("парсинг");
        assert_eq!((start, end), (None, None));
    }

    #[test]
    fn descending_defaults_to_desc_and_honors_asc() {
        assert!(query("created_at:desc", None).descending());
        assert!(query("created_at:DESC", None).descending());
        assert!(!query("created_at:asc", None).descending());
        assert!(!query("created_at:ASC", None).descending());
    }
}
