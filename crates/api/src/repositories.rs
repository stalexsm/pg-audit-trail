use std::collections::HashMap;

use serde_json::{Map, Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    AppResult,
    dto::{ChangeDetailDTO, ChangeRowDTO, TimeRange},
};

/// Число ключей в краткой сводке образов строки списка.
const CHANGES_SUMMARY_KEYS: i64 = 4;

/// Типизированный запрос списка журнала: вход репозитория. Всё, что
/// приходит извне, проверено и разобрано на уровне axum (axum-valid +
/// обработчик): здесь только чистые данные, без парсинга строк.
pub(crate) struct JournalQuery {
    pub page: i32,
    pub per_page: i32,
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
        q: &JournalQuery,
    ) -> AppResult<(Vec<ChangeRowDTO>, i64)> {
        let offset = (q.page - 1) * q.per_page;

        let mut builder = QueryBuilder::new(
            "
            SELECT
                c.id,
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
fn push_filters(builder: &mut QueryBuilder<Postgres>, q: &JournalQuery) {
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
