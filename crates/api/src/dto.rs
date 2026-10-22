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
/// Значения по чувствительным ключам словаря (ADR-0006) в сводке образов и
/// контексте маскируются — так же, как в детальной строке.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ChangeRowDTO {
    pub id: Uuid,
    /// Имя источника журнала; пустая строка — источник неизвестен (ADR-0005).
    pub source: String,
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
    #[serde(serialize_with = "mask_sensitive_data")]
    pub changes: serde_json::Value,
    #[schema(value_type = serde_json::Value)]
    #[serde(serialize_with = "mask_sensitive_data")]
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

    /// Имя источника журнала; строки без источника (до переезда на
    /// мультиисточность, пустая строка в журнале) в выборку не попадают;
    /// пустое значение параметра — без фильтра.
    pub source: Option<String>,

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
/// Значения по чувствительным ключам словаря (ADR-0006) в образах и
/// контексте маскируются — так же, как в списке.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct ChangeDetailDTO {
    pub id: Uuid,
    /// Имя источника журнала; пустая строка — источник неизвестен (ADR-0005).
    pub source: String,
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
    #[serde(serialize_with = "mask_sensitive_data")]
    pub context: serde_json::Value,
    pub committed_at: Option<DateTime<chrono::Utc>>,
    pub created_at: DateTime<chrono::Utc>,
}

/// Словарь чувствительных ключей (ADR-0006): значения по этим ключам в
/// образах «до/после» и контексте маскируются во всех точках выдачи —
/// и в списке, и в детальной строке. Сравнение ключей точное.
const SENSITIVE_KEYS: [&str; 6] = ["password", "passwd", "pwd", "token", "secret", "api_key"];

/// Маска чувствительного значения в образах и контексте.
///
/// Отдельна от `SOURCE_URL_MASK` (маска строки подключения реестра):
/// домены разные — значения журнала и url источника, — смена одной
/// маски не должна менять формат другой выдачи.
const SENSITIVE_VALUE_MASK: &str = "*****";

/// Ключ чувствителен по словарю (ADR-0006)?
fn is_sensitive_key(key: &str) -> bool {
    SENSITIVE_KEYS.contains(&key)
}

/// Единая точка маскирования журнала (ADR-0006): рекурсивно заменяет
/// значения по чувствительным ключам словаря маской — в образах «до/после»,
/// их краткой сводке списка и контексте приложения, заходя внутрь вложенных
/// объектов и массивов на любую глубину.
///
/// Краткая сводка списка (`{"key": ..., "before": ..., "after": ...}`)
/// обрабатывается как образ: чувствительное имя колонки в `key` скрывает
/// значения обоих образов целиком — сами значения лежат в ней не под
/// чувствительным ключом, а под `before`/`after`.
fn mask_sensitive_values(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let hidden_summary = map
                .get("key")
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_sensitive_key);

            map.iter()
                .map(|(key, val)| {
                    if is_sensitive_key(key)
                        || (hidden_summary && matches!(key.as_str(), "before" | "after"))
                    {
                        (
                            key.clone(),
                            serde_json::Value::String(SENSITIVE_VALUE_MASK.to_owned()),
                        )
                    } else {
                        (key.clone(), mask_sensitive_values(val))
                    }
                })
                .collect()
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(mask_sensitive_values).collect())
        }
        other => other.clone(),
    }
}

/// Сериализатор образов и контекста: пропускает значение через единую
/// точку маскирования (ADR-0006).
fn mask_sensitive_data<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    mask_sensitive_values(value).serialize(serializer)
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

// === Реестр источников (ADR-0005, тикет #28) ===

/// Префикс слота репликации: слот источника — `waltrail_<имя>`.
const SLOT_PREFIX: &str = "waltrail_";

/// Максимальная длина имени источника: слот `waltrail_<имя>` обязан
/// помещаться в NAMEDATALEN-1 (63 символа PostgreSQL) без молчаливой
/// усечки — иначе два длинных имени дали бы один слот.
const MAX_SOURCE_NAME_LEN: usize = 63 - SLOT_PREFIX.len();

/// Максимальная длина префикса MESSAGE: предел NAMEDATALEN-1 для префикса
/// `pg_logical_emit_message` (ADR-0004); u64 — под сигнатуру валидатора длины.
const MAX_MESSAGE_PREFIX_LEN: u64 = 63;

/// Границы размера батча: не меньше одной строки, верх — защита от
/// необдуманного батча на гигабайт (дефолт исторический — 500).
const MIN_BATCH_SIZE: i32 = 1;
const MAX_BATCH_SIZE: i32 = 10_000;

/// Источник журнала: строка реестра (ADR-0005). Поля статуса воркера
/// пишет воркер (тикет подхвата), API только читает.
#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct SourceDTO {
    pub name: String,
    /// Строка подключения в замаскированном виде: видны схема и хост;
    /// креды, порт и имя базы скрыты одной маской. Полный url передаётся
    /// только при регистрации и хранится в реестре для воркера, в ответах
    /// API не раскрывается (тикет #38); неизменяем после создания.
    #[schema(example = "postgresql://*****@db-client")]
    pub url: String,
    /// Служебные таблицы источника в виде `schema.table`, не попадающие в журнал.
    pub ignore_tables: Vec<String>,
    /// Префикс MESSAGE-канала контекста (ADR-0004).
    pub message_prefix: String,
    /// Размер чанка батчевой вставки строк в журнал.
    pub batch_size: i32,
    /// Состояние задачи воркера источника: `pending` — ждёт подхвата,
    /// `deleting` — помечен к удалению, воркер повторяет дроп слота и
    /// публикации у недоступного клиента (ADR-0005).
    pub worker_state: String,
    /// Когда воркер источника сохранил последнее изменение в журнал.
    pub last_event_at: Option<DateTime<chrono::Utc>>,
    /// Текст последней ошибки задачи воркера источника.
    pub last_error: Option<String>,
    /// Когда источник помечен к удалению; `NULL` — не помечен.
    pub deleted_at: Option<DateTime<chrono::Utc>>,
    pub created_at: DateTime<chrono::Utc>,
    pub updated_at: DateTime<chrono::Utc>,
}

/// Регистрация источника: POST /api/sources. Имя проверяется правилом
/// имён слотов репликации; URL неизменяем после создания.
#[derive(Deserialize, Debug, Validate, ToSchema)]
pub struct QSourceCreate {
    /// Имя источника: латиница нижним регистром, цифры, подчёркивание.
    /// Слот — `waltrail_<имя>`, публикация — `audit_<имя>` (ADR-0005).
    #[schema(max_length = 54)]
    #[validate(custom(function = "validate_slot_name"))]
    pub name: String,

    /// Строка подключения к базе клиента с кредами: схема postgres(q),
    /// непустой хост; неизменяема после создания.
    #[validate(custom(function = "validate_source_url"))]
    pub url: String,

    /// Служебные таблицы источника в виде `schema.table`; по умолчанию —
    /// таблицы типовых миграторов.
    #[serde(default = "default_ignore_tables")]
    #[validate(custom(function = "validate_ignore_tables"))]
    pub ignore_tables: Vec<String>,

    /// Префикс MESSAGE-канала контекста (ADR-0004); исторический дефолт — `_audit`.
    #[schema(max_length = 63)]
    #[serde(default = "default_message_prefix")]
    #[validate(length(min = 1, max = MAX_MESSAGE_PREFIX_LEN))]
    pub message_prefix: String,

    /// Размер чанка батчевой вставки строк в журнал; по умолчанию 500.
    #[schema(minimum = 1, maximum = 10_000)]
    #[serde(default = "default_batch_size")]
    #[validate(range(min = MIN_BATCH_SIZE, max = MAX_BATCH_SIZE))]
    pub batch_size: i32,
}

/// Правка настроек источника: PATCH /api/sources/{name}. Меняются только
/// служебные таблицы, префикс MESSAGE и размер батча; URL неизменяем
/// (ADR-0005: смена сервера — удалить и создать заново). Опущенное поле
/// остаётся без изменений.
#[derive(Deserialize, Debug, Validate, ToSchema)]
pub struct QSourcePatch {
    #[validate(custom(function = "validate_ignore_tables"))]
    pub ignore_tables: Option<Vec<String>>,

    #[schema(max_length = 63)]
    #[validate(length(min = 1, max = MAX_MESSAGE_PREFIX_LEN))]
    pub message_prefix: Option<String>,

    #[schema(minimum = 1, maximum = 10_000)]
    #[validate(range(min = MIN_BATCH_SIZE, max = MAX_BATCH_SIZE))]
    pub batch_size: Option<i32>,
}

/// Проверка имени источника правилом имён слотов репликации:
/// латиница нижним регистром, цифры, подчёркивание; не пусто и не длиннее
/// `MAX_SOURCE_NAME_LEN` (слот `waltrail_<имя>` обязан влезать в 63).
fn validate_slot_name(name: &str) -> Result<(), ValidationError> {
    let valid = !name.is_empty()
        && name.len() <= MAX_SOURCE_NAME_LEN
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');

    if valid {
        Ok(())
    } else {
        Err(ValidationError::new(
            "имя источника: латиница нижним регистром, цифры, подчёркивание; не пусто и не длиннее 54 символов — слот waltrail_<имя> обязан помещаться в 63 символа",
        ))
    }
}

/// Маска чувствительной части строки подключения в ответах API.
///
/// Отдельна от `SENSITIVE_VALUE_MASK` (маска образов и контекста журнала):
/// домены разные — url источника и значения журнала, — смена одной маски
/// не должна менять формат другой выдачи.
pub(crate) const SOURCE_URL_MASK: &str = "*****";

/// Маскирует строку подключения источника для ответов API: видны только
/// схема и хост; креды (пользователь и пароль), порт и имя базы скрыты
/// одной маской (тикет #38). Разобрать строку не удалось — в ответ уходит
/// голая маска: функция не имеет права вернуть вход как есть.
pub(crate) fn mask_source_url(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .filter(|parsed| parsed.host_str().is_some_and(|host| !host.is_empty()))
        .map(|parsed| {
            format!(
                "{}://{SOURCE_URL_MASK}@{}",
                parsed.scheme(),
                parsed.host_str().expect("хост проверен выше")
            )
        })
        .unwrap_or_else(|| SOURCE_URL_MASK.to_owned())
}

/// Проверка строки подключения: схема postgres(q), непустой хост.
fn validate_source_url(url: &str) -> Result<(), ValidationError> {
    let valid = url::Url::parse(url).is_ok_and(|parsed| {
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && parsed.host_str().is_some_and(|host| !host.is_empty())
    });

    if valid {
        Ok(())
    } else {
        Err(ValidationError::new(
            "строка подключения: postgresql://user:password@host:port/db",
        ))
    }
}

/// Проверка списка служебных таблиц: каждая — квалифицированное имя
/// `schema.table` без пробелов (воркер сопоставляет его с полным именем
/// отношения источника).
fn validate_ignore_tables(tables: &[String]) -> Result<(), ValidationError> {
    for table in tables {
        let valid = matches!(table.split_once('.'), Some((schema, name))
            if !schema.is_empty() && !name.is_empty() && !table.chars().any(char::is_whitespace));

        if !valid {
            return Err(ValidationError::new(
                "служебные таблицы — в виде schema.table, например public._sqlx_migrations",
            ));
        }
    }

    Ok(())
}

/// Служебные таблицы источника по умолчанию: типовые миграторы,
/// как в дефолте конфигурации воркера.
fn default_ignore_tables() -> Vec<String> {
    vec![
        "public.alembic_version".to_owned(),
        "public.schema_migrations".to_owned(),
        "public._sqlx_migrations".to_owned(),
        "public.django_migrations".to_owned(),
    ]
}

/// Префикс MESSAGE по умолчанию: исторический `_audit`.
fn default_message_prefix() -> String {
    "_audit".to_owned()
}

/// Размер батча по умолчанию.
fn default_batch_size() -> i32 {
    500
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(sortable: &str, created_at: Option<&str>) -> QChange {
        QChange {
            page: 1,
            per_page: 25,
            source: None,
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

    /// Тело создания источника с именем и URL, остальное — дефолты.
    fn create_body(name: &str, url: &str) -> QSourceCreate {
        serde_json::from_value(serde_json::json!({ "name": name, "url": url }))
            .expect("тело создания")
    }

    #[test]
    fn slot_name_accepts_lowercase_digits_underscore() {
        assert!(validate_slot_name("client_a").is_ok());
        assert!(validate_slot_name("a").is_ok());
        assert!(validate_slot_name("a1_").is_ok());
        assert!(validate_slot_name(&"a".repeat(54)).is_ok());

        assert!(validate_slot_name("").is_err());
        assert!(validate_slot_name("Client").is_err());
        assert!(validate_slot_name("with-dash").is_err());
        assert!(validate_slot_name("with space").is_err());
        assert!(validate_slot_name("кириллица").is_err());
        assert!(
            validate_slot_name(&"a".repeat(55)).is_err(),
            "слот не влезает в 63"
        );
    }

    #[test]
    fn source_url_requires_postgres_scheme_and_host() {
        assert!(validate_source_url("postgresql://user:pass@host:5432/db").is_ok());
        assert!(validate_source_url("postgres://host/db").is_ok());

        assert!(validate_source_url("").is_err());
        assert!(validate_source_url("not-a-url").is_err());
        assert!(validate_source_url("mysql://user:pass@host/db").is_err());
        assert!(validate_source_url("postgresql://").is_err(), "нет хоста");
    }

    #[test]
    fn ignore_tables_require_qualified_names() {
        assert!(
            validate_ignore_tables(&[
                "public.alembic_version".to_owned(),
                "app.schema_migrations".to_owned(),
                "private._log".to_owned(),
            ])
            .is_ok()
        );

        assert!(validate_ignore_tables(&["no_dot".to_owned()]).is_err());
        assert!(validate_ignore_tables(&["".to_owned()]).is_err());
        assert!(validate_ignore_tables(&["public.".to_owned()]).is_err());
        assert!(validate_ignore_tables(&[".table".to_owned()]).is_err());
        assert!(validate_ignore_tables(&["public .a".to_owned()]).is_err());
    }

    #[test]
    fn create_body_defaults_match_worker_defaults() {
        // Литералы дефолтов воркера (worker-cdc/src/config.rs) — на случай,
        // если чей-то дефолт изменится, а второй останется как был.
        let body = create_body("client_a", "postgresql://u:p@h:5432/db");
        assert_eq!(
            body.ignore_tables,
            vec![
                "public.alembic_version".to_owned(),
                "public.schema_migrations".to_owned(),
                "public._sqlx_migrations".to_owned(),
                "public.django_migrations".to_owned(),
            ]
        );
        assert_eq!(body.message_prefix, "_audit");
        assert_eq!(body.batch_size, 500);
    }

    #[test]
    fn create_body_validates() {
        assert!(
            create_body("client_a", "postgresql://u:p@h/db")
                .validate()
                .is_ok()
        );
        assert!(
            create_body("bad name", "postgresql://u:p@h/db")
                .validate()
                .is_err()
        );
        assert!(create_body("client_a", "mysql://h/db").validate().is_err());

        // Границы размера батча и длины префикса MESSAGE.
        let mut body = create_body("client_a", "postgresql://u:p@h/db");
        body.batch_size = MAX_BATCH_SIZE + 1;
        assert!(body.validate().is_err());
        body.batch_size = MIN_BATCH_SIZE - 1;
        assert!(body.validate().is_err());
        body.batch_size = MAX_BATCH_SIZE;
        assert!(body.validate().is_ok());
        body.message_prefix = "x".repeat(MAX_MESSAGE_PREFIX_LEN as usize + 1);
        assert!(body.validate().is_err());
        body.message_prefix = "x".repeat(MAX_MESSAGE_PREFIX_LEN as usize);
        assert!(body.validate().is_ok(), "верхняя граница включительна");
    }

    #[test]
    fn patch_validates_only_provided_fields() {
        // Пустая правка валидна: опущенные поля остаются без изменений.
        let patch: QSourcePatch = serde_json::from_str("{}").expect("пустая правка");
        assert!(patch.validate().is_ok());

        let patch: QSourcePatch =
            serde_json::from_str("{\"batch_size\": 100}").expect("правка батча");
        assert!(patch.validate().is_ok());

        let patch: QSourcePatch =
            serde_json::from_str("{\"message_prefix\": \"\"}").expect("пустой префикс");
        assert!(patch.validate().is_err());

        let patch: QSourcePatch =
            serde_json::from_str("{\"ignore_tables\": [\"no_dot\"]}").expect("таблицы");
        assert!(patch.validate().is_err());
    }

    #[test]
    fn patch_ignores_unknown_fields_like_url() {
        // Неизвестные поля тела (включая url) serde молча игнорирует:
        // неизменяемость URL проверяется интеграционным тестом шва.
        let patch: QSourcePatch = serde_json::from_value(serde_json::json!({
            "url": "postgresql://evil/x",
            "batch_size": 100,
        }))
        .expect("правка с лишним url");
        assert_eq!(patch.batch_size, Some(100));
        assert!(patch.validate().is_ok());
    }

    #[test]
    fn source_url_masks_credentials_port_and_database() {
        // Видны схема и хост; креды, порт и имя базы скрыты одной маской —
        // независимо от того, что из них было в исходной строке.
        assert_eq!(
            mask_source_url("postgresql://user:pass@db-client:5432/mydb"),
            "postgresql://*****@db-client"
        );
        assert_eq!(
            mask_source_url("postgres://user@db-client/db"),
            "postgres://*****@db-client"
        );
        assert_eq!(
            mask_source_url("postgresql://db-client"),
            "postgresql://*****@db-client"
        );
        assert_eq!(
            mask_source_url("postgresql://u:p@[::1]:5432/db"),
            "postgresql://*****@[::1]"
        );
    }

    #[test]
    fn source_url_mask_never_leaks_unparseable_string() {
        // Разобрать не удалось — в ответ уходит маска без деталей:
        // функция не имеет права вернуть вход как есть.
        assert_eq!(mask_source_url("not-a-url"), "*****");
        assert_eq!(mask_source_url(""), "*****");
    }

    // === Единое маскирование образов и контекста (ADR-0006, тикет #39) ===

    /// Сериализует DTO в JSON — так маскирование видит клиент API.
    fn serialized<T: serde::Serialize>(value: &T) -> serde_json::Value {
        serde_json::to_value(value).expect("сериализация DTO")
    }

    #[test]
    fn mask_covers_password_synonyms() {
        // Словарь из тикета #39, ключи перечислены литералами — независимо
        // от константы в коде.
        let masked = mask_sensitive_values(&serde_json::json!({
            "login": "alice",
            "password": "p-1",
            "passwd": "p-2",
            "pwd": "p-3",
            "token": "t-1",
            "secret": "s-1",
            "api_key": "k-1",
        }));

        assert_eq!(masked["login"], "alice");
        for key in ["password", "passwd", "pwd", "token", "secret", "api_key"] {
            assert_eq!(masked[key], "*****", "ключ {key} обязан быть замаскирован");
        }
    }

    #[test]
    fn mask_recurses_into_nested_objects_and_arrays() {
        let masked = mask_sensitive_values(&serde_json::json!({
            "user": {"login": "alice", "auth": {"token": "t-1"}},
            "items": [{"api_key": "k-1"}, {"note": "ок"}],
            "deep": {"a": {"b": {"passwd": "p-1"}}},
        }));

        assert_eq!(masked["user"]["login"], "alice");
        assert_eq!(masked["user"]["auth"]["token"], "*****");
        assert_eq!(masked["items"][0]["api_key"], "*****");
        assert_eq!(masked["items"][1], serde_json::json!({"note": "ок"}));
        assert_eq!(masked["deep"]["a"]["b"]["passwd"], "*****");
    }

    #[test]
    fn list_summary_hides_images_of_sensitive_key() {
        // Краткая сводка списка: значения образа чувствительной колонки
        // лежат под before/after — скрываются целиком; образы обычной
        // колонки остаются видимыми.
        let masked = mask_sensitive_values(&serde_json::json!([
            {"key": "password", "before": "old", "after": "new"},
            {"key": "name", "before": "John", "after": "Jane"},
        ]));

        assert_eq!(
            masked[0],
            serde_json::json!({"key": "password", "before": "*****", "after": "*****"})
        );
        assert_eq!(
            masked[1],
            serde_json::json!({"key": "name", "before": "John", "after": "Jane"})
        );
    }

    /// Одна запись журнала, как её видят обе точки выдачи: список — через
    /// `ChangeRowDTO`, деталь — через `ChangeDetailDTO`.
    fn one_record() -> (ChangeRowDTO, ChangeDetailDTO) {
        let image = serde_json::json!({
            "login": "alice",
            "password": "s3cret",
            "profile": {"api_key": "k-1"},
        });
        let context = serde_json::json!({"user": {"token": "t-1"}, "note": "ок"});
        let summary = serde_json::json!([
            {"key": "password", "before": "old", "after": "s3cret"},
        ]);

        let id = Uuid::new_v4();

        let row = ChangeRowDTO {
            id,
            source: String::new(),
            database: "client_db".to_string(),
            schema: "public".to_string(),
            table: "accounts".to_string(),
            operation: "UPDATE".to_string(),
            primary_key: None,
            trace_id: None,
            changes: summary,
            context: context.clone(),
            committed_at: None,
            created_at: chrono::Utc::now(),
        };
        let detail = ChangeDetailDTO {
            id,
            source: String::new(),
            database: "client_db".to_string(),
            schema: "public".to_string(),
            table: "accounts".to_string(),
            operation: "UPDATE".to_string(),
            primary_key: None,
            trace_id: None,
            before: image.clone(),
            after: image,
            context,
            committed_at: None,
            created_at: chrono::Utc::now(),
        };
        (row, detail)
    }

    #[test]
    fn list_and_detail_hide_sensitive_values_alike() {
        let (row, detail) = one_record();
        let (row, detail) = (serialized(&row), serialized(&detail));

        // Ни одно чувствительное значение не видно ни в списке, ни в детали.
        for leaked in ["s3cret", "k-1", "t-1", "old"] {
            assert!(!row.to_string().contains(leaked), "список утекает {leaked}");
            assert!(
                !detail.to_string().contains(leaked),
                "деталь утекает {leaked}"
            );
        }

        // Одинаковый результат маскирования для одной записи: одна и та же
        // маска у одного и того же значения в обеих точках выдачи.
        assert_eq!(detail["before"]["password"], detail["after"]["password"]);
        assert_eq!(
            detail["before"]["profile"]["api_key"],
            row["context"]["user"]["token"]
        );
        assert_eq!(row["changes"][0]["after"], detail["after"]["password"]);

        // Нечувствительные значения целы.
        assert_eq!(detail["before"]["login"], "alice");
        assert_eq!(detail["context"]["note"], "ок");
    }
}
