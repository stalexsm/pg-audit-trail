//! Интеграционный тест шва №2 «HTTP у api» (эпик #3, тикет #9).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d`. Роутер axum
//! проверяется против живого тестового Postgres; журнал сидируется чистым
//! SQL. Истина — HTTP-ответы: список журнала с пагинацией, сортировкой и
//! фильтрами (trace_id, таблица, операция, время, вхождение в `context`),
//! MESSAGE-строки наравне с изменениями, детальная, 404. Легаси (`audits`,
//! `request_id`) в тестовой схеме удалён — каждый зелёный запрос здесь
//! доказывает, что API к нему не обращается.

mod common;

use std::sync::Once;

use axum::http::StatusCode;
use chrono::{TimeZone, Utc};
use common::{
    SeedRow, audit_admin_url, cleanup_leftovers, connect_admin, connect_db, drop_db, get_json,
    insert_row, setup_journal, spawn_db,
};
use serde_json::{Value, json};
use uuid::Uuid;

use api::routes::api_router;

/// Логи внутри теста: ошибки SQL глушатся в репозитории и видны только
/// в трейсинге. Вызывать в начале каждого теста.
static INIT_LOGGING: Once = Once::new();
fn init_logging() {
    INIT_LOGGING.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("api=warn")),
            )
            .with_target(false)
            .with_test_writer()
            .init();
    });
}

/// Момент времени в журнале: 2025-01-день, 12:00 UTC + смещение в часах.
fn at(day: u32, hours: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 1, day, hours, 0, 0)
        .single()
        .expect("корректный момент времени")
}

/// Поднимает уникальную БД с журналом (миграции + снятие легаси)
/// и возвращает пул, имя БД и роутер с состоянием.
async fn spawn_env(prefix: &str) -> (sqlx::PgPool, String, axum::Router) {
    init_logging();

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

/// Строка изменения с заданной операцией, таблицей и контекстом.
fn row(operation: &str, table: &str, context: Value, created_at: chrono::DateTime<Utc>) -> SeedRow {
    let mut row = SeedRow::new(operation, created_at);
    row.table = table.to_string();
    row.context = context;
    row
}

#[tokio::test]
async fn list_paginates_and_sorts_by_created_at() {
    let (pool, db_name, router) = spawn_env("paginate").await;

    let mut early = row("CREATE", "orders", json!({}), at(10, 9));
    early.after = json!({"id": 1});
    let mut middle = row("UPDATE", "orders", json!({}), at(10, 11));
    middle.after = json!({"id": 2});
    let mut late = row("DELETE", "orders", json!({}), at(10, 13));
    late.after = json!({"id": 3});
    for r in [&early, &middle, &late] {
        insert_row(&pool, r).await;
    }

    // Первая страница: свежие сверху (сортировка по умолчанию).
    let (status, body) = get_json(router.clone(), "/api/changes?per_page=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["page"], 1);
    assert_eq!(body["pages"], 2);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 2);
    assert_eq!(body["rows"][0]["id"], json!(late.id));
    assert_eq!(body["rows"][1]["id"], json!(middle.id));

    // Вторая страница: остаётся самая ранняя строка.
    let (status, body) = get_json(router.clone(), "/api/changes?per_page=2&page=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["page"], 2);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["id"], json!(early.id));

    // Явное направление сортировки: старые сверху.
    let (status, body) = get_json(
        router.clone(),
        "/api/changes?per_page=2&sortable=created_at:asc",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["id"], json!(early.id));

    // Некорректный номер страницы нормализуется к первой.
    let (status, body) = get_json(router.clone(), "/api/changes?per_page=2&page=0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["page"], 1);
    assert_eq!(body["rows"][0]["id"], json!(late.id));

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn trace_id_returns_whole_operation() {
    let (pool, db_name, router) = spawn_env("trace").await;

    // Операция op-a: изменение строк и событие приложения с одним trace_id.
    let mut change = row("CREATE", "orders", json!({}), at(11, 9));
    change.trace_id = Some("op-a".to_string());
    change.after = json!({"id": 1});
    let event = SeedRow::message(json!({"trace_id": "op-a", "user": "alice"}), at(11, 9));

    // Чужая операция и строка без trace_id.
    let mut other = row("UPDATE", "orders", json!({}), at(11, 10));
    other.trace_id = Some("op-b".to_string());
    let orphan = row("DELETE", "orders", json!({}), at(11, 10));

    for r in [&change, &event, &other, &orphan] {
        insert_row(&pool, r).await;
    }

    let (status, body) = get_json(router.clone(), "/api/changes?trace_id=op-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pages"], 1);

    let ids: Vec<Value> = body["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| r["id"].clone())
        .collect();
    assert_eq!(ids.len(), 2, "вся операция: изменение и событие");
    assert!(ids.contains(&json!(change.id)));
    assert!(ids.contains(&json!(event.id)));

    // Событие приложения читается наравне с изменениями: строка MESSAGE
    // с payload в context и пустой сводкой образов.
    let message_row = body["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .find(|r| r["operation"] == "MESSAGE")
        .expect("MESSAGE-строка в списке");
    assert_eq!(
        message_row["context"],
        json!({"trace_id": "op-a", "user": "alice"})
    );
    assert_eq!(message_row["changes"], json!([]));

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn filters_by_table_operation_and_time() {
    let (pool, db_name, router) = spawn_env("filters").await;

    let orders = row("CREATE", "orders", json!({}), at(12, 9));
    let users = row("UPDATE", "users", json!({}), at(12, 11));
    let event = SeedRow::message(json!({"kind": "ping"}), at(12, 13));
    for r in [&orders, &users, &event] {
        insert_row(&pool, r).await;
    }

    // Таблица — без учёта регистра.
    let (status, body) = get_json(router.clone(), "/api/changes?table=ORDERS").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["table"], "orders");

    // Операция — без учёта регистра; MESSAGE — полноценный фильтр.
    let (status, body) = get_json(router.clone(), "/api/changes?operation=message").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["operation"], "MESSAGE");

    // Время: нижняя граница.
    let (status, body) = get_json(
        router.clone(),
        "/api/changes?created_at=2025-01-12T10:00:00Z",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["rows"].as_array().expect("rows").len(),
        2,
        "11:00 и 13:00"
    );

    // Время: диапазон с обеими границами.
    let (status, body) = get_json(
        router.clone(),
        "/api/changes?created_at=2025-01-12T09:00:00Z..2025-01-12T11:00:00Z",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["rows"].as_array().expect("rows").len(),
        2,
        "границы включены"
    );

    // Комбинация фильтров: таблица + время.
    let (status, body) = get_json(
        router.clone(),
        "/api/changes?table=users&created_at=2025-01-12T10:00:00Z..2025-01-12T12:00:00Z",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["table"], "users");

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn filter_by_context_containment() {
    let (pool, db_name, router) = spawn_env("context").await;

    let hit = row(
        "CREATE",
        "orders",
        json!({"app": "BORiY", "user": {"login": "alice", "role": {"name": "admin"}}, "row": 123}),
        at(13, 9),
    );
    let miss = row("CREATE", "orders", json!({"app": "Other"}), at(13, 9));
    for r in [&hit, &miss] {
        insert_row(&pool, r).await;
    }

    // Плоский ключ.
    let (status, body) = get_json(router.clone(), "/api/changes?app=BORiY").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["id"], json!(hit.id));

    // Вложенные ключи через точку.
    for uri in [
        "/api/changes?user.login=alice",
        "/api/changes?user.role.name=admin",
    ] {
        let (status, body) = get_json(router.clone(), uri).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["rows"].as_array().expect("rows").len(),
            1,
            "фильтр {uri} должен находить вложенный ключ"
        );
        assert_eq!(body["rows"][0]["id"], json!(hit.id));
    }

    // Числовое значение: совпадает и как число.
    let (status, body) = get_json(router.clone(), "/api/changes?row=123").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["id"], json!(hit.id));

    // Несовпадающее значение не находит ничего.
    let (status, body) = get_json(router.clone(), "/api/changes?user.login=bob").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 0);

    // Параметры-фильтры соединяются через AND.
    let (status, body) = get_json(router.clone(), "/api/changes?app=BORiY&user.login=bob").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 0);

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn detail_returns_full_row_and_masks_password() {
    let (pool, db_name, router) = spawn_env("detail").await;

    let mut change = row("UPDATE", "accounts", json!({"app": "BORiY"}), at(14, 9));
    change.primary_key = Some("id=7".to_string());
    change.trace_id = Some("op-c".to_string());
    change.before = json!({"id": 7, "name": "old", "password": "s3cret"});
    change.after = json!({"id": 7, "name": "new", "password": "n3w"});
    insert_row(&pool, &change).await;

    let (status, body) = get_json(router.clone(), &format!("/api/changes/{}", change.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], json!(change.id));
    assert_eq!(body["database"], "client_db");
    assert_eq!(body["schema"], "public");
    assert_eq!(body["table"], "accounts");
    assert_eq!(body["operation"], "UPDATE");
    assert_eq!(body["primary_key"], "id=7");
    assert_eq!(body["trace_id"], "op-c");
    // Пароль маскируется в обоих образах, остальные значения целы.
    assert_eq!(
        body["before"],
        json!({"id": 7, "name": "old", "password": "*****"})
    );
    assert_eq!(
        body["after"],
        json!({"id": 7, "name": "new", "password": "*****"})
    );
    assert_eq!(body["context"], json!({"app": "BORiY"}));
    assert_eq!(body["created_at"], "2025-01-14T09:00:00Z");
    assert_eq!(body["committed_at"], "2025-01-14T09:00:00Z");

    // MESSAGE-строка в детальной: payload целиком в context.
    let payload = json!({"trace_id": "op-c", "event": "login", "password": "p"});
    let event = SeedRow::message(payload, at(14, 9));
    insert_row(&pool, &event).await;

    let (status, body) = get_json(router.clone(), &format!("/api/changes/{}", event.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["operation"], "MESSAGE");
    assert_eq!(
        body["context"],
        json!({"trace_id": "op-c", "event": "login", "password": "p"}),
        "payload MESSAGE хранится дословно (ADR-0002)"
    );

    // Несуществующая запись — 404 с пояснением.
    let unknown = Uuid::new_v4();
    let (status, body) = get_json(router.clone(), &format!("/api/changes/{unknown}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["detail"]
            .as_str()
            .expect("detail")
            .contains("не существует")
    );

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn empty_journal_gives_empty_page() {
    let (_pool, db_name, router) = spawn_env("empty").await;

    let (status, body) = get_json(router, "/api/changes").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["page"], 1);
    assert_eq!(body["pages"], 0);
    assert_eq!(body["rows"], json!([]));

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}
