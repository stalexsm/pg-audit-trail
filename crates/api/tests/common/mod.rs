//! Общие хелперы живых интеграционных тестов api (шов №2, эпик #3).
//!
//! Каждый тестовый файл — отдельный крейт: часть хелперов в конкретном
//! файле не используется, поэтому dead_code здесь разрешён.

#![allow(dead_code)]

use std::sync::atomic::{AtomicI64, Ordering};

use axum::{Router, body::Body, http::Request, http::StatusCode};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

/// Подключение к тестовой БД аудита (db1).
pub fn audit_admin_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:54399/postgres".to_string())
}

/// Гигиена стенда: сносит тестовые БД прошлых прогонов. Паттерн — префикс
/// конкретного сценария: параллельные тесты не задевают БД друг друга.
pub async fn cleanup_leftovers(admin: &PgPool, pattern: &str) {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT datname FROM pg_database WHERE datname LIKE $1 AND datname != current_database()",
    )
    .bind(pattern)
    .fetch_all(admin)
    .await
    .unwrap_or_default();
    for name in names {
        // Имя БД — из pg_database того же стенда, без пользовательского ввода
        // (аудит sqlx::AssertSqlSafe).
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"
        )))
        .execute(admin)
        .await;
    }
}

/// Создаёт уникальную БД на администраторском пуле, возвращает её URL и имя.
pub async fn spawn_db(admin: &PgPool, base_url: &str, prefix: &str) -> (String, String) {
    let db_name = format!("{prefix}_{}", Uuid::new_v4().simple());
    // Имя БД генерируется из UUID здесь же — инъекции нет (аудит).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{db_name}\""
    )))
    .execute(admin)
    .await
    .expect("создание тестовой БД");

    let (prefix_url, _) = base_url
        .rsplit_once('/')
        .expect("URL должен содержать путь к БД");
    let url = format!("{prefix_url}/{db_name}");
    (url, db_name)
}

pub async fn drop_db(admin: &PgPool, db_name: &str) {
    // Имя БД — из фикстуры этого же теста, без пользовательского ввода (аудит).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
    )))
    .execute(admin)
    .await
    .expect("удаление тестовой БД");
}

/// Подключение к свежесозданной БД с ожиданием её готовности.
pub async fn connect_db(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(3)
        .connect(url)
        .await
        .expect("подключение к тестовой БД")
}

/// Подключение к администраторскому пулу стенда (db1).
pub async fn connect_admin(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите `docker compose up -d`")
}

/// Готовит журнал тестовой БД: применяет миграции.
pub async fn setup_journal(pool: &PgPool) {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await
        .expect("применение миграций");
}

/// Строка журнала для сидирования чистым SQL.
#[derive(Debug, Clone)]
pub struct SeedRow {
    pub id: Uuid,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub operation: String,
    pub primary_key: Option<String>,
    pub trace_id: Option<String>,
    pub before: Value,
    pub after: Value,
    pub context: Value,
    pub committed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl SeedRow {
    /// Строка изменения с пустыми образами и контекстом.
    pub fn new(operation: &str, created_at: DateTime<Utc>) -> Self {
        Self {
            id: Uuid::new_v4(),
            database: "client_db".to_string(),
            schema: "public".to_string(),
            table: "orders".to_string(),
            operation: operation.to_string(),
            primary_key: None,
            trace_id: None,
            before: serde_json::json!({}),
            after: serde_json::json!({}),
            context: serde_json::json!({}),
            committed_at: Some(created_at),
            created_at,
        }
    }
}

/// Вставляет строку журнала чистым SQL; позиция в WAL уникальна в пределах
/// процесса, чтобы у одинаковых строк не нарушался уникальный ключ журнала.
pub async fn insert_row(pool: &PgPool, row: &SeedRow) {
    static NEXT_POSITION: AtomicI64 = AtomicI64::new(1);
    let position = NEXT_POSITION.fetch_add(1, Ordering::SeqCst);

    sqlx::query(
        "INSERT INTO changes \
         (id, database, schema, \"table\", primary_key, operation, before, after, context, \
          trace_id, committed_at, created_at, transaction_id, position) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(row.id)
    .bind(&row.database)
    .bind(&row.schema)
    .bind(&row.table)
    .bind(&row.primary_key)
    .bind(&row.operation)
    .bind(&row.before)
    .bind(&row.after)
    .bind(&row.context)
    .bind(&row.trace_id)
    .bind(row.committed_at)
    .bind(row.created_at)
    .bind(1000_i64)
    .bind(position)
    .execute(pool)
    .await
    .expect("вставка строки журнала");
}

/// GET-запрос к роутеру API; возвращает статус и разобранный JSON-ответ.
pub async fn get_json(router: Router, uri: &str) -> (StatusCode, Value) {
    let (status, body) = get_raw(router, uri).await;
    let json = serde_json::from_str(&body).expect("JSON-ответ");
    (status, json)
}

/// Запрос с методом и JSON-телом к роутеру API: статус и разобранный JSON.
/// Пустое тело ответа (например, 202 на удаление) разбирается в `Null`.
pub async fn send_json(
    router: Router,
    method: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = router
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("запрос"),
        )
        .await
        .expect("ответ роутера");

    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("тело ответа");
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&String::from_utf8_lossy(&bytes)).expect("JSON-ответ")
    };
    (status, json)
}

/// GET-запрос к роутеру API без разбора тела: rejection-и axum (400)
/// отвечают не во всех случаях JSON'ом.
pub async fn get_raw(router: Router, uri: &str) -> (StatusCode, String) {
    let response = router
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("запрос"),
        )
        .await
        .expect("ответ роутера");

    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("тело ответа");
    (status, String::from_utf8_lossy(&body).into_owned())
}
