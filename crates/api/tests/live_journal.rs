//! Интеграционный тест шва №2 «HTTP у api» (эпик #3, тикеты #9 и #20).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d`. Роутер axum
//! проверяется против живого тестового Postgres; журнал сидируется чистым
//! SQL. Истина — HTTP-ответы: список журнала с пагинацией, сортировкой и
//! фильтрами (источник, trace_id, таблица, операция, время, вхождение в
//! `context`), только изменения строк (ADR-0004), детальная, 404.

mod common;

use std::sync::Once;

use axum::http::StatusCode;
use chrono::{TimeZone, Utc};
use common::{
    SeedRow, audit_admin_url, cleanup_leftovers, connect_admin, connect_db, drop_db, get_json,
    get_raw, insert_row, setup_journal, spawn_db,
};
use serde_json::{Value, json};
use uuid::Uuid;

use api::routes::api_router;

/// Логи внутри теста: видны трейсы на случай отладки. Вызывать в начале
/// каждого теста.
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

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn invalid_query_params_give_400() {
    let (_pool, db_name, router) = spawn_env("badparams").await;

    // Всё, что приходит извне, проверяется на уровне axum: любое нарушение
    // параметров — 400, а не молчаливое игнорирование или нормализация.
    let cases = [
        "/api/changes?created_at=not-a-date",
        "/api/changes?created_at=2025-01-01..wrong",
        "/api/changes?page=0",
        "/api/changes?per_page=0",
        "/api/changes?per_page=500",
        "/api/changes?sortable=huh",
        "/api/changes?sortable=committed_at:asc",
    ];
    for uri in cases {
        let (status, body) = get_raw(router.clone(), uri).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "ожидался 400 для {uri}: {body}"
        );
    }

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

/// Верхняя граница номера страницы из валидации api/dto.rs (`MAX_PAGE`);
/// в тесте повторяется литералом — шов проверяется снаружи, через HTTP.
const MAX_PAGE_PARAM: i32 = 1_000_000;

#[tokio::test]
async fn huge_page_gives_empty_page_not_500() {
    let (pool, db_name, router) = spawn_env("hugepage").await;

    let mut early = row("CREATE", "orders", json!({}), at(16, 9));
    early.after = json!({"id": 1});
    insert_row(&pool, &early).await;

    // Экстремальный, но валидный номер страницы: смещение считается в i64
    // без переполнения, ответ — корректная пустая страница (200), а не 500.
    let (status, body) = get_json(
        router.clone(),
        &format!("/api/changes?page={MAX_PAGE_PARAM}&per_page=200"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["page"], MAX_PAGE_PARAM);
    assert_eq!(body["pages"], 1);
    assert_eq!(body["rows"], json!([]));

    // За верхней границей номера страницы — 400 валидации, без похода в БД.
    let (status, body) = get_json(
        router,
        &format!("/api/changes?page={}&per_page=200", MAX_PAGE_PARAM + 1),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "тело: {body}");

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn trace_id_returns_whole_operation() {
    let (pool, db_name, router) = spawn_env("trace").await;

    // Операция op-a: два изменения строк одной операции (создание и
    // последующее обновление той же строки заказа).
    let mut create = row("CREATE", "orders", json!({}), at(11, 9));
    create.trace_id = Some("op-a".to_string());
    create.after = json!({"id": 1});
    let mut update = row("UPDATE", "orders", json!({}), at(11, 9));
    update.trace_id = Some("op-a".to_string());
    update.after = json!({"id": 1});

    // Чужая операция и строка без trace_id.
    let mut other = row("UPDATE", "orders", json!({}), at(11, 10));
    other.trace_id = Some("op-b".to_string());
    let orphan = row("DELETE", "orders", json!({}), at(11, 10));

    for r in [&create, &update, &other, &orphan] {
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
    assert_eq!(
        ids.len(),
        2,
        "вся операция: только изменения строк (ADR-0004)"
    );
    assert!(ids.contains(&json!(create.id)));
    assert!(ids.contains(&json!(update.id)));

    // В журнале нет MESSAGE-строк: единственное содержимое журнала —
    // изменения строк; событие приложения без изменений строк в него
    // не попадает.
    for r in body["rows"].as_array().expect("rows") {
        assert_ne!(r["operation"], "MESSAGE");
    }

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn filters_by_table_operation_and_time() {
    let (pool, db_name, router) = spawn_env("filters").await;

    let orders = row("CREATE", "orders", json!({}), at(12, 9));
    let users = row("UPDATE", "users", json!({}), at(12, 11));
    let truncate = row("TRUNCATE", "events", json!({}), at(12, 13));
    for r in [&orders, &users, &truncate] {
        insert_row(&pool, r).await;
    }

    // Таблица — без учёта регистра.
    let (status, body) = get_json(router.clone(), "/api/changes?table=ORDERS").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["table"], "orders");

    // Операция — без учёта регистра; TRUNCATE — полноценный фильтр.
    let (status, body) = get_json(router.clone(), "/api/changes?operation=truncate").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(body["rows"][0]["operation"], "TRUNCATE");

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
async fn masks_sensitive_keys_recursively_in_list_and_detail() {
    let (pool, db_name, router) = spawn_env("detail").await;

    let mut change = row(
        "UPDATE",
        "accounts",
        json!({"app": "BORiY", "auth": {"token": "t-1"}}),
        at(14, 9),
    );
    change.primary_key = Some("id=7".to_string());
    change.trace_id = Some("op-c".to_string());
    change.before = json!({"id": 7, "name": "old", "password": "s3cret", "api_key": "k-0"});
    change.after = json!({"id": 7, "name": "new", "password": "n3w", "api_key": "k-1"});
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
    // Словарь чувствительных ключей (ADR-0006): пароль и синонимы
    // маскируются в обоих образах, вложенный токен контекста — тоже;
    // остальные значения целы.
    assert_eq!(
        body["before"],
        json!({"id": 7, "name": "old", "password": "*****", "api_key": "*****"})
    );
    assert_eq!(
        body["after"],
        json!({"id": 7, "name": "new", "password": "*****", "api_key": "*****"})
    );
    assert_eq!(
        body["context"],
        json!({"app": "BORiY", "auth": {"token": "*****"}})
    );
    assert_eq!(body["created_at"], "2025-01-14T09:00:00Z");
    assert_eq!(body["committed_at"], "2025-01-14T09:00:00Z");

    // Список маскирует ту же запись так же: сводка образов скрывает
    // значения чувствительных колонок, контекст — рекурсивно.
    let (status, body) = get_json(router.clone(), "/api/changes").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().expect("rows").len(), 1);
    let listed = &body["rows"][0];
    assert_eq!(listed["id"], json!(change.id));
    for entry in listed["changes"].as_array().expect("сводка образов") {
        let key = entry["key"].as_str().expect("имя колонки");
        if matches!(key, "password" | "api_key") {
            assert_eq!(entry["before"], "*****", "сводка: {key}.до");
            assert_eq!(entry["after"], "*****", "сводка: {key}.после");
        } else {
            assert_ne!(entry["before"], "*****", "сводка: {key}.до цело");
        }
    }
    assert_eq!(
        listed["context"],
        json!({"app": "BORiY", "auth": {"token": "*****"}})
    );
    for leaked in ["s3cret", "n3w", "k-0", "k-1", "t-1"] {
        assert!(
            !listed.to_string().contains(leaked),
            "список утекает {leaked}"
        );
    }

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

#[tokio::test]
async fn filter_by_source() {
    let (pool, db_name, router) = spawn_env("source").await;

    // Два источника (ADR-0005): имена баз у клиентов совпадают, разводит
    // их только ключ источника. Третья строка — до переезда, источник
    // неизвестен (пустая строка, как у всех строк старого пайплайна).
    let mut alpha = row("CREATE", "orders", json!({}), at(15, 9));
    alpha.source = "client_alpha".to_string();
    let mut beta = row("CREATE", "orders", json!({}), at(15, 10));
    beta.source = "client_beta".to_string();
    let legacy = row("UPDATE", "orders", json!({}), at(15, 11));
    for r in [&alpha, &beta, &legacy] {
        insert_row(&pool, r).await;
    }

    // Фильтр по источнику возвращает только его строки, источник виден
    // и в строке ответа.
    let (status, body) = get_json(router.clone(), "/api/changes?source=client_alpha").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pages"], 1);
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "только строки client_alpha");
    assert_eq!(rows[0]["id"], json!(alpha.id));
    assert_eq!(rows[0]["source"], "client_alpha");

    // Второй источник не путается с первым, хотя базы у клиентов
    // называются одинаково.
    let (status, body) = get_json(router.clone(), "/api/changes?source=client_beta").await;
    assert_eq!(status, StatusCode::OK);
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "только строки client_beta");
    assert_eq!(rows[0]["id"], json!(beta.id));

    // Пустое значение параметра — как без фильтра, а не «только
    // безымянные»: именованные источники не прячутся случайно.
    let (status, body) = get_json(router.clone(), "/api/changes?source=&per_page=200").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["rows"].as_array().expect("rows").len(),
        3,
        "пустой параметр источника — отсутствие фильтра"
    );

    // Без фильтра читается весь журнал, включая старые строки.
    let (status, body) = get_json(router.clone(), "/api/changes").await;
    assert_eq!(status, StatusCode::OK);
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 3, "все источники и строки до переезда");

    // Несуществующий источник — пустая страница, а не ошибка.
    let (status, body) = get_json(router.clone(), "/api/changes?source=no_such_source").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"], json!([]));

    // Комбинация с другим фильтром.
    let mut users = row("UPDATE", "users", json!({}), at(15, 12));
    users.source = "client_alpha".to_string();
    insert_row(&pool, &users).await;
    let (status, body) = get_json(
        router.clone(),
        "/api/changes?source=client_alpha&table=USERS",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "источник + таблица, без учёта регистра");
    assert_eq!(rows[0]["id"], json!(users.id));

    // Источник виден и в детальной строке.
    let (status, body) = get_json(router.clone(), &format!("/api/changes/{}", beta.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "client_beta");

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}
