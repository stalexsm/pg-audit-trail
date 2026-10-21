//! Общие хелперы живых интеграционных тестов worker-cdc.
//!
//! Каждый тестовый файл — отдельный крейт: часть хелперов в конкретном
//! файле не используется, поэтому dead_code здесь разрешён.

#![allow(dead_code)]

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Подключение к тестовой БД аудита (db1).
pub fn audit_admin_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:54399/postgres".to_string())
}

/// Подключение к тестовому источнику CDC (db-src).
pub fn source_admin_url() -> String {
    std::env::var("TEST_SOURCE_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:54398/postgres".to_string())
}

/// Гигиена стенда: сносит тестовые БД прошлых прогонов (вместе с их слотами —
/// DROP DATABASE FORCE на PG 16+ удаляет и логические слоты). Незакрытые
/// после падения прогонов слоты, привязанные к чужим БД, блокировали бы
/// START_REPLICATION нового прогона. Паттерн — префикс конкретного сценария:
/// параллельные тесты не задевают БД друг друга.
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

/// Создаёт уникальную БД на администраторском пуле, возвращает её URL.
pub async fn spawn_db(admin: &PgPool, base_url: &str, prefix: &str) -> (String, String) {
    let db_name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
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

/// Готовит источник: публикация `audit` на все таблицы и тестовые таблицы.
pub async fn setup_source(source: &PgPool) {
    sqlx::query("CREATE PUBLICATION audit FOR ALL TABLES")
        .execute(source)
        .await
        .expect("создание публикации");

    sqlx::query(
        "CREATE TABLE orders (
            id BIGINT PRIMARY KEY,
            amount NUMERIC(10,2),
            note TEXT,
            created_at TIMESTAMPTZ
        )",
    )
    .execute(source)
    .await
    .expect("создание orders");
    sqlx::query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(source)
        .await
        .expect("REPLICA IDENTITY FULL для orders");

    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY, kind TEXT)")
        .execute(source)
        .await
        .expect("создание events");

    sqlx::query("CREATE TABLE schema_migrations (version BIGINT PRIMARY KEY, name TEXT)")
        .execute(source)
        .await
        .expect("создание schema_migrations");

    // Составной ключ: primary_key должен собраться из key-флагов Relation.
    sqlx::query("CREATE TABLE composite (a INT, b INT, payload TEXT, PRIMARY KEY (a, b))")
        .execute(source)
        .await
        .expect("создание composite");
    sqlx::query("ALTER TABLE composite REPLICA IDENTITY FULL")
        .execute(source)
        .await
        .expect("REPLICA IDENTITY FULL для composite");
}

/// Ждёт, пока предикат не выполнится (опрос БД аудита).
pub async fn wait_until<F, Fut>(mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..120 {
        if predicate().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("не дождались ожидаемого состояния журнала за 60 секунд");
}

/// Считает replay-дубли журнала: пары `(commit_lsn, event_index)`,
/// сохранённые более одного раза (тесты на выживание).
pub async fn journal_duplicate_pairs(audit: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM ( \
             SELECT commit_lsn, event_index FROM changes \
             WHERE commit_lsn IS NOT NULL \
             GROUP BY commit_lsn, event_index HAVING count(*) > 1 \
         ) dupes",
    )
    .fetch_one(audit)
    .await
    .expect("подсчёт дублирующихся пар (commit_lsn, event_index)")
}

/// Подключение к свежесозданной БД с ожиданием её готовности.
pub async fn connect_db(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(3)
        .connect(url)
        .await
        .expect("подключение к тестовой БД")
}
