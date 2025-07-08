use axum::extract::{Path, Query, State};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder};
use tracing::error;
use uuid::Uuid;

use crate::dto::{AuditDTO, ChangeDTO, ChangeDetailDTO, QAudit, QChange};

// Функция для добавления условий в запрос
fn add_conditions<'a>(query_builder: &mut QueryBuilder<'a, Postgres>, q: &'a QAudit) {
    // Вспомогательная функция для форматирования поиска с подстроками
    let like_format = |val: &str| format!("%{val}%");

    // Макрос для уменьшения дублирования кода при добавлении условий
    macro_rules! add_condition {
        ($field:expr, $operator:expr, $value:expr) => {
            query_builder
                .push(" AND ")
                .push($field)
                .push($operator)
                .push_bind($value);
        };
        ($field:expr, $operator:expr, $value:expr, $format:expr) => {
            query_builder
                .push(" AND ")
                .push($field)
                .push($operator)
                .push_bind($format($value));
        };
    }

    if let Some(val) = &q.event {
        add_condition!("a.event ", " ILIKE ", val, like_format);
    }

    if let Some(val) = &q.primary_key {
        add_condition!("a.primary_key ", " = ", val);
    }

    if let Some(val) = &q.status_code {
        add_condition!("a.status_code ", " = ", val);
    }

    if let Some(val) = &q.app {
        add_condition!("a.context->>'app' ", " ILIKE ", val, like_format);
    }

    if let Some(val) = &q.ip {
        add_condition!("a.context->>'ip' ", " = ", val);
    }

    if let Some(val) = &q.method {
        add_condition!("LOWER(a.context->>'method') ", " = ", val.to_lowercase());
    }

    if let Some(val) = &q.user_login {
        add_condition!(
            "LOWER(a.context->'user'->>'login') ",
            " = ",
            val.to_lowercase()
        );
    }

    if let Some(val) = &q.user_fio {
        add_condition!("a.context->'user'->>'fio'", " ILIKE ", val, like_format);
    }

    if let Some(val) = &q.user_role_name {
        add_condition!(
            "a.context->'user'->'role'->>'name'",
            " ILIKE ",
            val,
            like_format
        );
    }

    if let Some(val) = &q.created_at {
        if let Some((start, end)) = val.split_once("..") {
            if let Ok(dt) = start.parse::<DateTime<Utc>>() {
                add_condition!("a.created_at", " >= ", dt);
            }

            if let Ok(dt) = end.parse::<DateTime<Utc>>() {
                add_condition!("a.created_at", " <= ", dt);
            }
        } else if let Ok(dt) = val.parse::<DateTime<Utc>>() {
            add_condition!("a.created_at", " >= ", dt);
        }
    }
}

#[derive(Debug)]
pub(crate) struct AuditRepo;

impl AuditRepo {
    pub(crate) async fn get_list_paginate(
        State(pool): State<PgPool>,
        Query(q): Query<QAudit>,
    ) -> (Vec<AuditDTO>, i64) {
        let offset = (q.page - 1) * q.per_page;
        let per_page = q.per_page;

        let mut query_builder = QueryBuilder::new(
            "
                SELECT
                    a.id,
                    a.event,
                    a.primary_key,
                    a.status_code,
                    a.context,
                    a.created_at,
                    EXISTS (
                        SELECT 1
                        FROM changes AS c
                        WHERE c.request_id = a.id::text
                    ) AS is_change
                FROM audits AS a WHERE true
                ",
        );

        add_conditions(&mut query_builder, &q);

        // Обработка сортировки
        if let Some((_, order)) = q.sortable.split_once(':') {
            let direction = match order.to_lowercase().as_str() {
                "desc" => "DESC",
                _ => "ASC", // По умолчанию используем ASC для всех других значений
            };

            query_builder
                .push(" ORDER BY a.created_at ")
                .push(direction);
        } else {
            // Стандартная сортировка, если формат не соответствует ожидаемому
            query_builder.push(" ORDER BY a.created_at DESC");
        }

        // --- OFFSET / LIMIT ---
        query_builder.push(" OFFSET ").push_bind(offset);
        query_builder.push(" LIMIT ").push_bind(per_page);

        let rows: Vec<AuditDTO> = query_builder
            .build_query_as()
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|_| vec![]);

        let mut query_builder = QueryBuilder::new(
            "
                SELECT COUNT(a.id) FROM audits AS a WHERE true
                ",
        );

        add_conditions(&mut query_builder, &q);

        let cnt: i64 = query_builder
            .build_query_scalar()
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| {
                error!(error=%e, "Ошибка!");
                0
            });

        let pages = (cnt as f64 / per_page as f64).ceil() as i64;

        (rows, pages)
    }

    // Метод для получения response_body.
    pub(crate) async fn get_body(
        State(pool): State<PgPool>,
        Path(id): Path<Uuid>,
    ) -> Option<String> {
        sqlx::query_scalar::<_, String>("SELECT response_body FROM audits WHERE id = $1;")
            .bind(id)
            .fetch_optional(&pool)
            .await
            .unwrap_or_else(|e| {
                error!(error=%e, "Ошибка запроса в бд!");
                None
            })
    }
}

#[derive(Debug)]
pub(crate) struct ChangeRepo;

impl ChangeRepo {
    pub async fn get_list_paginate(
        State(pool): State<PgPool>,
        Path(id): Path<Uuid>,
        Query(q): Query<QChange>,
    ) -> (Vec<ChangeDTO>, i64) {
        let mut current = Map::new();
        let mut conditions: Vec<Value> = Vec::new();

        let items: Vec<(String, String)> = q.filters.into_iter().collect();
        if !items.is_empty() {
            generate_combinations(&items, 0, &mut current, &mut conditions);
        }

        let mut query_builder = QueryBuilder::new(
            "
            SELECT
                c.id,
                c.database,
                c.table,
                c.operation,
                (
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
                        LIMIT 4
                    ) AS k
                ) AS changes
            FROM changes c",
        );

        query_builder
            .push(" WHERE c.request_id = ")
            .push_bind(id.to_string());

        if let Some(tbl) = &q.table {
            query_builder
                .push(" AND LOWER(c.table) = ")
                .push_bind(tbl.to_lowercase());
        }

        if let Some(op) = &q.operation {
            query_builder
                .push(" AND LOWER(c.operation) = ")
                .push_bind(op.to_lowercase());
        }

        if !conditions.is_empty() {
            query_builder.push(" AND ((");

            let mut first = true;
            for c in &conditions {
                if !first {
                    query_builder.push(" OR ");
                } else {
                    first = false;
                }
                query_builder.push("c.before @> ").push_bind(c.clone());
            }

            query_builder.push(") OR (");

            let mut first = true;
            for c in &conditions {
                if !first {
                    query_builder.push(" OR ");
                } else {
                    first = false;
                }
                query_builder.push("c.after @> ").push_bind(c.clone());
            }

            query_builder.push("))");
        }

        query_builder.push(" OFFSET ").push_bind(q.offset);
        query_builder.push(" LIMIT ").push_bind(q.limit);

        let rows = query_builder
            .build_query_as()
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| {
                error!(error=%e, "E");
                vec![]
            });

        // Подсчет строк
        let mut query_builder = QueryBuilder::new(
            "
            SELECT
                COUNT(*)
            FROM
                changes AS c ",
        );

        query_builder
            .push(
                " WHERE
            c.request_id = ",
            )
            .push_bind(id.to_string());

        if let Some(tbl) = &q.table {
            query_builder
                .push(" AND LOWER(c.table) = ")
                .push_bind(tbl.to_lowercase());
        }

        if let Some(op) = &q.operation {
            query_builder
                .push(" AND LOWER(c.operation) = ")
                .push_bind(op.to_lowercase());
        }

        if !conditions.is_empty() {
            query_builder.push(" AND ((");

            let mut first = true;
            for c in &conditions {
                if !first {
                    query_builder.push(" OR ");
                } else {
                    first = false;
                }
                query_builder.push("c.before @> ").push_bind(c.clone());
            }

            query_builder.push(") OR (");

            let mut first = true;
            for c in &conditions {
                if !first {
                    query_builder.push(" OR ");
                } else {
                    first = false;
                }
                query_builder.push("c.after @> ").push_bind(c.clone());
            }

            query_builder.push("))");
        }

        let cnt = query_builder
            .build_query_scalar()
            .fetch_one(&pool)
            .await
            .unwrap_or(0);

        (rows, cnt)
    }

    pub async fn get_by_id(
        State(pool): State<PgPool>,
        Path(id): Path<Uuid>,
    ) -> Option<ChangeDetailDTO> {
        sqlx::query_as(
            "
            SELECT
                c.id,
                c.database,
                c.schema,
                c.table,
                c.operation,
                c.primary_key,
                c.before,
                c.after,
                c.context
            FROM
                changes AS c
            WHERE
                c.id = $1;
          ",
        )
        .bind(id)
        .fetch_optional(&pool)
        .await
        .unwrap_or_else(|e| {
            error!("Failed to fetch change by id: {}", e);
            None
        })
    }
}

// Функция которая собирает все варианты...
fn generate_combinations(
    items: &[(String, String)],
    index: usize,
    current: &mut serde_json::Map<String, serde_json::Value>,
    conditions: &mut Vec<Value>,
) {
    if index == items.len() {
        let json_val = serde_json::Value::Object(current.clone());
        conditions.push(json_val);
        return;
    }

    let (key, value) = &items[index];
    // Всегда добавляем строковый вариант
    current.insert(key.clone(), json!(value));
    generate_combinations(items, index + 1, current, conditions);

    // Если значение можно распарсить как число, добавляем и числовой вариант
    if let Ok(num) = value.parse::<i64>() {
        current.insert(key.clone(), json!(num));
        generate_combinations(items, index + 1, current, conditions);
    }
    // Возвращаемся назад (backtrack)
    current.remove(key);
}
