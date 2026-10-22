//! Интеграционный тест формата ответов 5xx (тикет #45).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d`. Роутер axum
//! проверяется против живого тестового Postgres: срыв запроса к БД
//! (снятая таблица журнала) даёт 5xx с нейтральным сообщением и
//! correlation id; тексты sqlx/PostgreSQL и имена внутренних объектов
//! клиенту не раскрываются. Осмысленные 4xx (конфликт имени источника)
//! сохраняются как есть (см. `post_rejects_duplicate_name` в
//! `live_sources.rs`).

mod common;

use axum::http::StatusCode;
use common::{
    audit_admin_url, cleanup_leftovers, connect_admin, connect_db, drop_db, get_json,
    setup_journal, spawn_db,
};
use uuid::Uuid;

use api::routes::api_router;

/// Поднимает уникальную БД с журналом и возвращает пул, имя БД и роутер.
async fn spawn_env(prefix: &str) -> (sqlx::PgPool, String, axum::Router) {
    let admin = connect_admin(&audit_admin_url()).await;
    cleanup_leftovers(&admin, &format!("waltrail_api_{prefix}_%")).await;

    let (url, db_name) = spawn_db(
        &admin,
        &audit_admin_url(),
        &format!("waltrail_api_{prefix}"),
    )
    .await;
    let pool = connect_db(&url).await;
    setup_journal(&pool).await;

    (pool.clone(), db_name, api_router().with_state(pool))
}

#[tokio::test]
async fn server_error_hides_infrastructure_details() {
    let (pool, db_name, router) = spawn_env("errors5xx").await;

    // Срыв слоя данных: таблица журнала снята — запрос обязан дать 5xx.
    sqlx::query("DROP TABLE changes")
        .execute(&pool)
        .await
        .expect("снятие таблицы журнала");

    let (status, body) = get_json(router, "/api/changes").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // Нейтральное сообщение и correlation id (UUID) — по нему ошибка
    // находится в логе; ключ — тот же `detail`, что у 4xx.
    assert_eq!(body["detail"], "Внутренняя ошибка сервера");
    let correlation_id = body["correlation_id"].as_str().expect("correlation_id");
    Uuid::parse_str(correlation_id).expect("correlation id — UUID");

    // Тексты sqlx/PostgreSQL и имена внутренних объектов не раскрываются.
    let raw = body.to_string();
    for leaked in [
        "error returned from database",
        "relation \"",
        "does not exist",
        "changes",
        &db_name,
    ] {
        assert!(
            !raw.contains(leaked),
            "5xx-ответ раскрывает {leaked}: {raw}"
        );
    }

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}
