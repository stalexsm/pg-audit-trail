//! Фатальные ошибки супервизора (тикет #47): недоступность БД аудита и
//! провал миграций завершают `worker_cdc::run` ошибкой — `main` отображает
//! её в ненулевой код выхода процесса, чтобы перезапускающая система
//! отреагировала. Тесты закрепляют контракт библиотеки, на который опирается
//! `main.rs`.
//!
//! Первый тест инфраструктуры не требует (адрес в никуда); второй использует
//! TEST_DATABASE_URL: конфликтующая схема в уникальной БД срывает первую
//! миграцию.

mod common;

use std::time::Duration;

use common::{audit_admin_url, cleanup_leftovers, connect_db, drop_db, spawn_db};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use worker_cdc::WorkerConfig;

/// Подключение к администраторскому пулу стенда (db1).
async fn connect_admin(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите инфраструктуру тестов")
}

fn config(database_url: String) -> WorkerConfig {
    WorkerConfig {
        database_url,
        reconnect_initial_backoff_ms: 1_000,
        reconnect_max_backoff_ms: 30_000,
    }
}

/// AC (тикет #47): недоступность БД аудита на старте — фатальная ошибка:
/// `run` завершается `Err` (внутри цикла согласования недоступность БД
/// лишь переносит тик, но соединение на старте обязано состояться).
#[tokio::test]
async fn unreachable_audit_db_fails_run() {
    let config = config("postgresql://postgres:postgres@127.0.0.1:1/none".to_owned());
    // Таймаут-обёртка: ошибка обязана прийти, а не зависнуть навсегда;
    // acquire_timeout пула супервизора — 30 с, берём запас.
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        worker_cdc::run(config, tokio_util::sync::CancellationToken::new()),
    )
    .await
    .expect("run завершился за 60 секунд");

    assert!(
        result.is_err(),
        "недоступная БД аудита — фатальная ошибка супервизора: {result:?}"
    );
}

/// AC (тикет #47): провал миграций — фатальная ошибка: `run` завершается
/// `Err`, а не продолжает работу на незнакомой схеме.
#[tokio::test]
async fn failed_migrations_fail_run() {
    let admin = connect_admin(&audit_admin_url()).await;
    cleanup_leftovers(&admin, "waltrail_audit_fatal_%").await;
    let (url, db_name) = spawn_db(&admin, &audit_admin_url(), "waltrail_audit_fatal").await;

    // Ловушка для первой миграции: индекс `changes_after_index` уже занят,
    // CREATE INDEX (без IF NOT EXISTS) падает — миграции срываются.
    let db = connect_db(&url).await;
    sqlx::query("CREATE TABLE changes (id INT, junk JSONB)")
        .execute(&db)
        .await
        .expect("создание таблицы-ловушки");
    sqlx::query("CREATE INDEX changes_after_index ON changes (junk)")
        .execute(&db)
        .await
        .expect("создание индекса-ловушки");
    drop(db);

    let result = worker_cdc::run(
        config(url.clone()),
        tokio_util::sync::CancellationToken::new(),
    )
    .await;
    assert!(
        result.is_err(),
        "провал миграций — фатальная ошибка супервизора: {result:?}"
    );

    drop_db(&admin, &db_name).await;
}
