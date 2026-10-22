use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    AppError, AppResult,
    dto::{ChangeDetailDTO, ChangeRowDTO, SourceDTO, TimeRange, mask_source_url},
};

/// Число ключей в краткой сводке образов строки списка.
const CHANGES_SUMMARY_KEYS: i64 = 4;

/// Типизированный запрос списка изменений: вход репозитория. Всё, что
/// приходит извне, проверено и разобрано на уровне axum (axum-valid +
/// обработчик): здесь только чистые данные, без парсинга строк.
pub(crate) struct ChangeQuery {
    pub page: i32,
    pub per_page: i32,
    /// Имя источника журнала; `None` — без фильтра (все источники).
    pub source: Option<String>,
    pub trace_id: Option<String>,
    pub table: Option<String>,
    pub operation: Option<String>,
    /// Диапазон времени сохранения (нижняя и верхняя границы).
    pub time_range: TimeRange,
    /// Направление сортировки: `true` — от свежих к старым.
    pub descending: bool,
    /// Фильтры вхождения в `context`.
    pub context_filters: HashMap<String, String>,
}

/// Репозиторий журнала: единственная сущность — `changes` (ADR-0002).
///
/// Читаются только изменения строк: MESSAGE строк журнала не создаёт
/// (ADR-0004), он лишь обогащает их `context` и `trace_id`.
#[derive(Debug)]
pub(crate) struct ChangeRepo;

impl ChangeRepo {
    /// Страница журнала и число страниц.
    ///
    /// Сортировка — по времени сохранения в журнал; направление задаётся
    /// параметром `sortable` (`created_at:asc|desc`, по умолчанию desc).
    /// Ошибка БД — ошибкой запроса (500), а не пустой страницей.
    pub(crate) async fn get_list_paginate(
        pool: &PgPool,
        q: &ChangeQuery,
    ) -> AppResult<(Vec<ChangeRowDTO>, i64)> {
        let offset = (q.page - 1) * q.per_page;

        let mut builder = QueryBuilder::new(
            "
            SELECT
                c.id,
                c.source,
                c.database,
                c.schema,
                c.\"table\",
                c.operation,
                c.primary_key,
                c.trace_id,
                COALESCE((
                    SELECT jsonb_agg(jsonb_build_object(
                        'key', k.key,
                        'before', c.before -> k.key,
                        'after', c.after -> k.key
                    ))
                    FROM (
                        SELECT key
                        FROM (
                            SELECT key FROM jsonb_each(c.before)
                            UNION
                            SELECT key FROM jsonb_each(c.after)
                        ) AS keys
                        GROUP BY key
                        ORDER BY key
                        LIMIT ",
        );
        builder.push_bind(CHANGES_SUMMARY_KEYS);
        builder.push(
            "
                    ) AS k
                ), '[]'::jsonb) AS changes,
                c.context,
                c.committed_at,
                c.created_at
            FROM changes AS c
            WHERE true
            ",
        );

        push_filters(&mut builder, q);

        // Сортировка по времени сохранения. Тай-брейк по id: TIMESTAMPTZ(0)
        // даёт совпадения внутри секунды, без него порядок строк страницы
        // недетерминирован.
        let direction = if q.descending { "DESC" } else { "ASC" };
        builder.push(" ORDER BY c.created_at ").push(direction);
        builder.push(", c.id ").push(direction);

        builder.push(" OFFSET ").push_bind(offset);
        builder.push(" LIMIT ").push_bind(q.per_page);

        let rows: Vec<ChangeRowDTO> = builder.build_query_as().fetch_all(pool).await?;

        let mut builder = QueryBuilder::new("SELECT COUNT(*) FROM changes AS c WHERE true");
        push_filters(&mut builder, q);

        let cnt: i64 = builder.build_query_scalar().fetch_one(pool).await?;

        let pages = (cnt as f64 / q.per_page as f64).ceil() as i64;

        Ok((rows, pages))
    }

    /// Детальная строка журнала по идентификатору; ошибка БД — ошибкой
    /// запроса (500), отличие от `Ok(None)` — записи нет (404).
    pub(crate) async fn get_by_id(pool: &PgPool, id: Uuid) -> AppResult<Option<ChangeDetailDTO>> {
        let found = sqlx::query_as(
            "
            SELECT
                c.id,
                c.source,
                c.database,
                c.schema,
                c.\"table\",
                c.operation,
                c.primary_key,
                c.trace_id,
                c.before,
                c.after,
                c.context,
                c.committed_at,
                c.created_at
            FROM changes AS c
            WHERE c.id = $1;
            ",
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        Ok(found)
    }
}

/// Добавляет условия фильтров к запросу со сквозным псевдонимом `c`.
fn push_filters(builder: &mut QueryBuilder<Postgres>, q: &ChangeQuery) {
    // Фильтр по источнику — равенство: имена источников нормированы
    // правилом слотов `[a-z0-9_]` (ADR-0005). Пустая строка (источник
    // неизвестен, строки до переезда) под конкретное имя не попадает.
    if let Some(val) = &q.source {
        builder.push(" AND c.source = ").push_bind(val);
    }

    // Фильтр «вся операция» — равенство по колонке trace_id (ADR-0004):
    if let Some(val) = &q.trace_id {
        builder.push(" AND c.trace_id = ").push_bind(val);
    }

    if let Some(val) = &q.table {
        builder
            .push(" AND LOWER(c.\"table\") = ")
            .push_bind(val.to_lowercase());
    }

    if let Some(val) = &q.operation {
        builder
            .push(" AND LOWER(c.operation) = ")
            .push_bind(val.to_lowercase());
    }

    // Диапазон времени сохранения: границы уже разобраны на входе.
    if let Some(start) = q.time_range.0 {
        builder.push(" AND c.created_at >= ").push_bind(start);
    }
    if let Some(end) = q.time_range.1 {
        builder.push(" AND c.created_at <= ").push_bind(end);
    }

    // Вхождение в контекст (GIN-индекс `changes_context_index`): варианты
    // одного параметра соединяются OR, разные параметры — AND.
    for variants in context_conditions(&q.context_filters) {
        if variants.is_empty() {
            continue;
        }
        builder.push(" AND (");
        for (i, cond) in variants.iter().enumerate() {
            if i > 0 {
                builder.push(" OR ");
            }
            builder.push("c.context @> ").push_bind(cond.clone());
        }
        builder.push(")");
    }
}

/// JSON-условия вхождения в `context` для параметров запроса: по группе
/// (вектору вариантов) на параметр. Внутри группы — OR (варианты одного
/// значения), между группами — AND.
///
/// Точка в имени параметра задаёт вложенность (`user.login` →
/// `{"user": {"login": ...}}`). Значение, парсящееся как целое число,
/// даёт два варианта — строковый и числовой: в JSONB числа бывают
/// представленными обоими способами.
fn context_conditions(filters: &HashMap<String, String>) -> Vec<Vec<Value>> {
    let mut groups = Vec::new();

    for (path, value) in filters {
        let segments: Vec<&str> = path.split('.').collect();

        let mut variants = Vec::new();
        let mut obj = Map::new();
        insert_nested(&mut obj, &segments, json!(value));
        variants.push(Value::Object(obj));

        if let Ok(num) = value.parse::<i64>() {
            let mut obj = Map::new();
            insert_nested(&mut obj, &segments, json!(num));
            variants.push(Value::Object(obj));
        }

        groups.push(variants);
    }

    groups
}

/// Вставляет значение по вложенному пути в JSON-объект, создавая
/// промежуточные уровни.
fn insert_nested(obj: &mut Map<String, Value>, path: &[&str], value: Value) {
    match path {
        [key] => {
            obj.insert((*key).to_owned(), value);
        }
        [key, rest @ ..] => {
            let entry = obj
                .entry((*key).to_owned())
                .or_insert_with(|| Value::Object(Map::new()));
            if !entry.is_object() {
                *entry = Value::Object(Map::new());
            }
            if let Value::Object(inner) = entry {
                insert_nested(inner, rest, value);
            }
        }
        [] => {}
    }
}

// === Реестр источников (ADR-0005, тикет #28) ===

/// Колонки реестра источников для SELECT/RETURNING — точно в порядке полей
/// `SourceDTO`. Инвариант безопасности (тикет #38): url в каждой строке,
/// построенной из этой выборки, маскируется (`with_masked_url`) перед
/// выдачей из репозитория — новая выборка обязана пройти через ту же
/// маску, иначе утечёт полный url в ответ API (страховка — live-тесты
/// реестра, проверяющие все ответы).
const SOURCE_COLUMNS: &str = "name, url, ignore_tables, message_prefix, batch_size, \
    worker_state, last_event_at, last_error, deleted_at, created_at, updated_at";

/// Вход репозитория: регистрация источника (POST /api/sources). Валидность
/// полей проверена на уровне axum; здесь — чистые данные.
pub(crate) struct SourceCreate {
    pub name: String,
    pub url: String,
    pub ignore_tables: Vec<String>,
    pub message_prefix: String,
    pub batch_size: i32,
}

/// Вход репозитория: правка настроек источника (PATCH). `Some` — поле
/// меняется, `None` — остаётся без изменений; URL неизменяем (ADR-0005)
/// и здесь не представлен.
pub(crate) struct SourcePatch {
    pub ignore_tables: Option<Vec<String>>,
    pub message_prefix: Option<String>,
    pub batch_size: Option<i32>,
}

/// Маскирует url в реестровой строке перед выдачей из репозитория:
/// полный url не покидает слой данных и не уходит в ответы API (тикет #38).
fn with_masked_url(mut source: SourceDTO) -> SourceDTO {
    source.url = mask_source_url(&source.url);
    source
}

/// Репозиторий реестра источников: единственная сущность — `sources`
/// (ADR-0005). Статус воркера только читается: пишет его воркер.
#[derive(Debug)]
pub(crate) struct SourceRepo;

impl SourceRepo {
    /// Регистрирует источник; статус воркера — дефолт схемы (`pending`).
    /// Дубликат имени — ошибка валидации (400), прочая ошибка БД — 500.
    pub(crate) async fn create(pool: &PgPool, source: &SourceCreate) -> AppResult<SourceDTO> {
        let mut builder = QueryBuilder::new(
            "INSERT INTO sources (name, url, ignore_tables, message_prefix, batch_size) \
             VALUES ($1, $2, $3, $4, $5) RETURNING ",
        );
        builder.push(SOURCE_COLUMNS);

        builder
            .build_query_as()
            .bind(&source.name)
            .bind(&source.url)
            .bind(&source.ignore_tables)
            .bind(&source.message_prefix)
            .bind(source.batch_size)
            .fetch_one(pool)
            .await
            .map(with_masked_url)
            .map_err(map_create_error)
    }

    /// Все источники реестра со статусами воркеров, по имени.
    pub(crate) async fn get_list(pool: &PgPool) -> AppResult<Vec<SourceDTO>> {
        let mut builder = QueryBuilder::new("SELECT ");
        builder
            .push(SOURCE_COLUMNS)
            .push(" FROM sources ORDER BY name");

        let rows = builder.build_query_as().fetch_all(pool).await?;

        Ok(rows.into_iter().map(with_masked_url).collect())
    }

    /// Один источник по имени; `None` — источника нет (404).
    pub(crate) async fn get_by_name(pool: &PgPool, name: &str) -> AppResult<Option<SourceDTO>> {
        let mut builder = QueryBuilder::new("SELECT ");
        builder
            .push(SOURCE_COLUMNS)
            .push(" FROM sources WHERE name = ")
            .push_bind(name);

        let found = builder.build_query_as().fetch_optional(pool).await?;

        Ok(found.map(with_masked_url))
    }

    /// Меняет только переданные настройки источника; помеченный к удалению
    /// не меняется (`None` — 404/409 решает хендлер). Дата правки обновляется
    /// всегда, даже при пустой правке.
    pub(crate) async fn update(
        pool: &PgPool,
        name: &str,
        patch: &SourcePatch,
    ) -> AppResult<Option<SourceDTO>> {
        let mut builder = QueryBuilder::new("UPDATE sources SET updated_at = NOW()");

        if let Some(tables) = &patch.ignore_tables {
            builder.push(", ignore_tables = ").push_bind(tables);
        }
        if let Some(prefix) = &patch.message_prefix {
            builder.push(", message_prefix = ").push_bind(prefix);
        }
        if let Some(batch_size) = patch.batch_size {
            builder.push(", batch_size = ").push_bind(batch_size);
        }

        builder
            .push(" WHERE name = ")
            .push_bind(name)
            .push(" AND deleted_at IS NULL")
            .push(format!(" RETURNING {SOURCE_COLUMNS}"));

        let updated = builder.build_query_as().fetch_optional(pool).await?;

        Ok(updated.map(with_masked_url))
    }

    /// Помечает источник к удалению (ADR-0005: исполнение — за воркером);
    /// повторная пометка не перетирает дату. Источника нет — `None` (404).
    pub(crate) async fn mark_deleted(pool: &PgPool, name: &str) -> AppResult<Option<()>> {
        let marked = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "UPDATE sources SET deleted_at = COALESCE(deleted_at, NOW()) \
             WHERE name = $1 RETURNING deleted_at",
        )
        .bind(name)
        .fetch_optional(pool)
        .await?;

        Ok(marked.map(|_| ()))
    }
}

/// Нарушение уникальности имени источника (23505) — ошибка валидации (400),
/// прочая ошибка БД — 500.
fn map_create_error(err: sqlx::Error) -> AppError {
    if err
        .as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code.as_ref() == "23505")
    {
        return AppError::bad_request("Имя источника уже занято");
    }

    AppError::from(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_filter_builds_nested_containment() {
        let mut filters = HashMap::new();
        filters.insert("user.login".to_string(), "alice".to_string());

        assert_eq!(
            context_conditions(&filters),
            vec![vec![json!({"user": {"login": "alice"}})]]
        );
    }

    #[test]
    fn flat_filter_builds_flat_containment() {
        let mut filters = HashMap::new();
        filters.insert("app".to_string(), "BORiY".to_string());

        assert_eq!(
            context_conditions(&filters),
            vec![vec![json!({"app": "BORiY"})]]
        );
    }

    #[test]
    fn numeric_filter_gives_string_and_number_variants() {
        let mut filters = HashMap::new();
        filters.insert("row_id".to_string(), "42".to_string());

        assert_eq!(
            context_conditions(&filters),
            vec![vec![json!({"row_id": "42"}), json!({"row_id": 42})]]
        );
    }

    #[test]
    fn several_filters_are_independent_groups() {
        let mut filters = HashMap::new();
        filters.insert("app".to_string(), "x".to_string());
        filters.insert("ip".to_string(), "1.2.3.4".to_string());

        let groups = context_conditions(&filters);
        assert_eq!(groups.len(), 2, "по группе на параметр");
        let flat: Vec<Value> = groups.into_iter().flatten().collect();
        assert!(flat.contains(&json!({"app": "x"})));
        assert!(flat.contains(&json!({"ip": "1.2.3.4"})));
    }

    #[test]
    fn empty_filters_give_no_conditions() {
        assert!(context_conditions(&HashMap::new()).is_empty());
    }

    #[test]
    fn insert_nested_creates_missing_levels() {
        let mut obj = Map::new();
        insert_nested(&mut obj, &["a", "b", "c"], json!(1));
        assert_eq!(
            obj,
            json!({"a": {"b": {"c": 1}}}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn insert_nested_overwrites_non_object_intermediate() {
        let mut obj = json!({"a": "str"}).as_object().unwrap().clone();
        insert_nested(&mut obj, &["a", "b"], json!(1));
        assert_eq!(Value::Object(obj), json!({"a": {"b": 1}}));
    }

    #[test]
    fn insert_nested_keeps_existing_object() {
        let mut obj = json!({"a": {"kept": true}}).as_object().unwrap().clone();
        insert_nested(&mut obj, &["a", "b"], json!(1));
        assert_eq!(Value::Object(obj), json!({"a": {"kept": true, "b": 1}}));
    }
}
