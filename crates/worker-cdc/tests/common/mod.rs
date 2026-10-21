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

/// Готовит источник: тестовые таблицы. Публикацию не создаёт: бутстрап
/// воркера обязан сам создать публикацию и слот при подхвате источника
/// (ADR-0005) — это часть проверяемого поведения.
pub async fn setup_source(source: &PgPool) {
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

/// Уникальное имя источника реестра для прогона: правило имён слотов
/// `[a-z0-9_]` (ADR-0005), длина влезает в колонку (54) — из имени
/// деривируются слот и публикация.
pub fn source_name(prefix: &str) -> String {
    let id = &uuid::Uuid::new_v4().simple().to_string()[..12];
    format!("{prefix}_{id}")
}

/// Слот логической репликации источника, деривированный из имени (ADR-0005).
pub fn source_slot(name: &str) -> String {
    format!("waltrail_{name}")
}

/// Публикация источника на клиенте, деривированная из имени (ADR-0005).
pub fn source_publication(name: &str) -> String {
    format!("audit_{name}")
}

/// application_name walsender-соединения задачи источника (replicate.rs).
pub fn application_name(slot: &str) -> String {
    format!("waltrail-cdc-{slot}")
}

/// Регистрирует источник в реестре БД аудита (как POST /api/sources):
/// неизменяемые поля — имя и URL, остальное — дефолты схемы.
pub async fn register_source(audit: &PgPool, name: &str, url: &str) {
    sqlx::query("INSERT INTO sources (name, url) VALUES ($1, $2)")
        .bind(name)
        .bind(url)
        .execute(audit)
        .await
        .expect("регистрация источника в реестре");
}

/// Регистрирует источник с явными настройками (игнор-таблицы, префикс
/// MESSAGE, размер батча) — для сценариев, проверяющих чтение настроек
/// из реестра.
pub async fn register_source_with(
    audit: &PgPool,
    name: &str,
    url: &str,
    ignore_tables: &[&str],
    message_prefix: &str,
    batch_size: i32,
) {
    let tables: Vec<String> = ignore_tables.iter().map(|t| t.to_string()).collect();
    sqlx::query(
        "INSERT INTO sources (name, url, ignore_tables, message_prefix, batch_size) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(name)
    .bind(url)
    .bind(&tables)
    .bind(message_prefix)
    .bind(batch_size)
    .execute(audit)
    .await
    .expect("регистрация источника в реестре");
}

/// Ждёт, пока воркер применит миграции к БД аудита (таблица реестра).
pub async fn wait_sources_table(audit: &PgPool) {
    wait_until(|| async {
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('public.sources') IS NOT NULL")
            .fetch_one(audit)
            .await
            .unwrap_or(false)
    })
    .await;
}

/// Ждёт, пока у источника появится слот репликации (бутстрап воркера).
pub async fn wait_slot(source: &PgPool, slot: &str) {
    let slot = slot.to_owned();
    wait_until(|| async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
        )
        .bind(&slot)
        .fetch_one(source)
        .await
        .unwrap_or(false)
    })
    .await;
}

/// Ждёт, пока у источника появится публикация (бутстрап воркера).
pub async fn wait_publication(source: &PgPool, publication: &str) {
    let publication = publication.to_owned();
    wait_until(|| async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
        )
        .bind(&publication)
        .fetch_one(source)
        .await
        .unwrap_or(false)
    })
    .await;
}

/// Ждёт, пока задача источника перейдёт в заданное состояние реестра
/// (`sources.worker_state` — пишет воркер).
pub async fn wait_source_state(audit: &PgPool, name: &str, state: &str) {
    let expected = state.to_owned();
    let source = name.to_owned();
    wait_until(|| async {
        sqlx::query_scalar::<_, Option<String>>("SELECT worker_state FROM sources WHERE name = $1")
            .bind(&source)
            .fetch_one(audit)
            .await
            .ok()
            .flatten()
            .as_deref()
            == Some(expected.as_str())
    })
    .await;
}
