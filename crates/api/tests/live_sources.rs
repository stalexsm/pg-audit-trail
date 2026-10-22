//! Интеграционный тест шва «HTTP у api»: реестр источников (ADR-0005,
//! тикет #28).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d`. Роутер axum
//! проверяется против живого тестового Postgres. Истина — HTTP-ответы:
//! регистрация (201, дефолты, ошибки валидации на дубликат и невалидное
//! имя/URL), список и чтение одного со статусом воркера, частичная правка
//! настроек с неизменяемым URL, удаление как пометка (202).

mod common;

use std::sync::Once;

use axum::http::StatusCode;
use common::{
    audit_admin_url, cleanup_leftovers, connect_admin, connect_db, drop_db, get_json, send_json,
    spawn_db,
};
use serde_json::{Value, json};
use sqlx::Row;

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
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("применение миграций");

    (pool.clone(), db_name, api_router().with_state(pool))
}

/// Тело регистрации: имя и строка подключения, остальное — дефолты.
fn create_body(name: &str, url: &str) -> Value {
    json!({ "name": name, "url": url })
}

/// Регистрирует источник; тело ответа — источник.
async fn create_source(router: axum::Router, name: &str) -> Value {
    let (status, body) = send_json(
        router,
        "POST",
        "/api/sources",
        create_body(name, &format!("postgresql://user:pass@{name}-host:5432/db")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "регистрация {name}: {body}");

    body
}

#[tokio::test]
async fn post_creates_source_with_defaults() {
    let (pool, db_name, router) = spawn_env("sources_create").await;

    let (status, body) = send_json(
        router.clone(),
        "POST",
        "/api/sources",
        create_body("client_a", "postgresql://user:pass@host:5432/db"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Ответ — реестровая строка целиком: настройки, статус воркера.
    // URL в ответе маскирован (тикет #38): видны схема и хост; креды,
    // порт и имя базы скрыты. Полный url — только в БД (для воркера).
    assert_eq!(body["name"], "client_a");
    assert_eq!(body["url"], "postgresql://*****@host");
    assert_eq!(
        body["ignore_tables"],
        json!([
            "public.alembic_version",
            "public.schema_migrations",
            "public._sqlx_migrations",
            "public.django_migrations"
        ]),
        "дефолт — служебные таблицы типовых миграторов"
    );
    assert_eq!(body["message_prefix"], "_audit");
    assert_eq!(body["batch_size"], 500);
    assert_eq!(
        body["worker_state"], "pending",
        "новый источник ждёт подхвата"
    );
    assert_eq!(body["last_event_at"], Value::Null);
    assert_eq!(body["last_error"], Value::Null);
    assert_eq!(body["deleted_at"], Value::Null);
    assert!(body["created_at"].is_string());
    assert!(body["updated_at"].is_string());

    // Строка реестра реально в БД: ключ источника и статус по умолчанию;
    // полный url сохранён как был — инжест не меняется (тикет #38).
    let row = sqlx::query("SELECT worker_state, deleted_at, url FROM sources WHERE name = $1")
        .bind("client_a")
        .fetch_one(&pool)
        .await
        .expect("строка реестра");
    assert_eq!(row.get::<String, _>("worker_state"), "pending");
    assert!(
        row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("deleted_at")
            .is_none()
    );
    assert_eq!(
        row.get::<String, _>("url"),
        "postgresql://user:pass@host:5432/db"
    );

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn post_rejects_duplicate_name() {
    let (_pool, db_name, router) = spawn_env("sources_dup").await;

    create_source(router.clone(), "client_a").await;

    let (status, body) = send_json(
        router.clone(),
        "POST",
        "/api/sources",
        create_body("client_a", "postgresql://user:pass@other:5432/db"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "дубликат — 400: {body}");
    assert!(body["detail"].as_str().expect("detail").contains("занято"));

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn post_rejects_invalid_name_and_url() {
    let (_pool, db_name, router) = spawn_env("sources_bad").await;

    // Правило имён слотов репликации: латиница нижним регистром, цифры,
    // подчёркивание; слот waltrail_<имя> обязан влезать в 63 символа.
    let bad_names = [
        "Client".to_owned(),
        "with-dash".to_owned(),
        "with space".to_owned(),
        "кириллица".to_owned(),
        String::new(),
        "a".repeat(55),
    ];
    for name in bad_names {
        let (status, body) = send_json(
            router.clone(),
            "POST",
            "/api/sources",
            create_body(&name, "postgresql://user:pass@host:5432/db"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "имя `{name}` — 400: {body}"
        );
    }

    // Строка подключения: схема postgres(q) и непустой хост.
    let bad_urls = ["not-a-url", "mysql://user:pass@host/db", "postgresql://"];
    for url in bad_urls {
        let (status, body) = send_json(
            router.clone(),
            "POST",
            "/api/sources",
            create_body("client_a", url),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "URL `{url}` — 400: {body}");
    }

    // Ничего из отклонённого не попало в реестр.
    let (status, body) = get_json(router, "/api/sources").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().expect("sources").len(), 0);

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn list_and_one_return_registry_fields_with_worker_status() {
    let (_pool, db_name, router) = spawn_env("sources_read").await;

    create_source(router.clone(), "client_a").await;
    create_source(router.clone(), "client_b").await;

    // Список: оба источника, по имени.
    let (status, body) = get_json(router.clone(), "/api/sources").await;
    assert_eq!(status, StatusCode::OK);
    let list = body.as_array().expect("sources");
    assert_eq!(
        list.iter()
            .map(|s| s["name"].as_str().expect("name"))
            .collect::<Vec<_>>(),
        vec!["client_a", "client_b"]
    );
    for source in list {
        assert!(
            source["worker_state"].is_string(),
            "статус воркера в списке"
        );
        assert!(source["last_event_at"].is_null());
        assert!(source["last_error"].is_null());
        // Ни один ответ реестра не раскрывает креды (тикет #38).
        let url = source["url"].as_str().expect("url");
        let name = source["name"].as_str().expect("name");
        assert_eq!(url, format!("postgresql://*****@{name}-host"));
        assert!(!url.contains("pass"), "пароль не виден в списке: {url}");
    }

    // Чтение одного.
    let (status, body) = get_json(router.clone(), "/api/sources/client_a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "client_a");
    assert_eq!(body["url"], "postgresql://*****@client_a-host");
    assert_eq!(body["batch_size"], 500);

    // Неизвестное имя — 404.
    let (status, _) = get_json(router, "/api/sources/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

/// Статусы, записанные воркером (подключен/ошибка, время последнего
/// события, текст ошибки), видны в ответах API — тикет #30. Воркер в тесте
/// имитируется прямой записью в реестр — тем же UPDATE, что делает
/// StatusWriter; шов проверяет проход значений из БД в HTTP-ответ.
#[tokio::test]
async fn worker_written_statuses_are_visible_in_api_responses() {
    let (pool, db_name, router) = spawn_env("sources_status").await;

    create_source(router.clone(), "client_a").await;

    // Воркер: источник подключён, время последнего события зафиксировано.
    sqlx::query(
        "UPDATE sources SET worker_state = 'connected', last_event_at = now() \
         WHERE name = $1",
    )
    .bind("client_a")
    .execute(&pool)
    .await
    .expect("статус connected от воркера");

    let (status, body) = get_json(router.clone(), "/api/sources/client_a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker_state"], "connected");
    assert!(
        body["last_event_at"].is_string(),
        "время последнего события в ответе: {}",
        body["last_event_at"]
    );
    assert_eq!(body["last_error"], Value::Null);

    // Воркер: попытка не удалась — состояние и текст ошибки в реестре.
    sqlx::query(
        "UPDATE sources SET worker_state = 'error', \
         last_error = 'подключение к БД источника не удалось' WHERE name = $1",
    )
    .bind("client_a")
    .execute(&pool)
    .await
    .expect("статус error от воркера");

    let (status, body) = get_json(router, "/api/sources/client_a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker_state"], "error");
    assert_eq!(
        body["last_error"], "подключение к БД источника не удалось",
        "текст ошибки виден в ответе API"
    );

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn patch_updates_only_provided_fields_and_keeps_url() {
    let (pool, db_name, router) = spawn_env("sources_patch").await;

    create_source(router.clone(), "client_a").await;

    // Частичная правка: меняется только размер батча, опущенные поля на месте.
    let (status, body) = send_json(
        router.clone(),
        "PATCH",
        "/api/sources/client_a",
        json!({ "batch_size": 100 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "правка батча: {body}");
    assert_eq!(body["batch_size"], 100);
    assert_eq!(body["message_prefix"], "_audit");
    assert_eq!(body["url"], "postgresql://*****@client_a-host");

    // URL неизменяем (ADR-0005): попытка передать поле url — явный 400,
    // правка целиком не применяется (тикет #54): раньше поле молча
    // игнорировалось, и клиент верил, что сменил сервер.
    let (status, body) = send_json(
        router.clone(),
        "PATCH",
        "/api/sources/client_a",
        json!({
            "url": "postgresql://user:pass@evil:5432/db",
            "ignore_tables": ["public.kv_migrations"],
            "message_prefix": "_ctx"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "правка с url: {body}");
    assert!(
        body["detail"]
            .as_str()
            .is_some_and(|d| d.contains("неизменяемо")),
        "диагностика называет неизменяемость url: {body}"
    );

    // Правка без url применяется как обычно; пустой список служебных
    // таблиц — валидная очистка.
    let (status, body) = send_json(
        router.clone(),
        "PATCH",
        "/api/sources/client_a",
        json!({
            "ignore_tables": ["public.kv_migrations"],
            "message_prefix": "_ctx"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "правка настроек: {body}");
    assert_eq!(body["url"], "postgresql://*****@client_a-host");
    assert_eq!(body["ignore_tables"], json!(["public.kv_migrations"]));
    assert_eq!(body["message_prefix"], "_ctx");
    assert_eq!(body["batch_size"], 100, "опущенное поле осталось");

    // Ошибка валидации тела — 400.
    let (status, _) = send_json(
        router.clone(),
        "PATCH",
        "/api/sources/client_a",
        json!({ "message_prefix": "" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Неизвестное имя — 404.
    let (status, _) = send_json(
        router.clone(),
        "PATCH",
        "/api/sources/nope",
        json!({ "batch_size": 100 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // В БД — те же значения, что и в ответах.
    let row = sqlx::query(
        "SELECT url, message_prefix, batch_size, ignore_tables FROM sources WHERE name = $1",
    )
    .bind("client_a")
    .fetch_one(&pool)
    .await
    .expect("строка реестра");
    assert_eq!(
        row.get::<String, _>("url"),
        "postgresql://user:pass@client_a-host:5432/db"
    );
    assert_eq!(row.get::<String, _>("message_prefix"), "_ctx");
    assert_eq!(row.get::<i32, _>("batch_size"), 100);

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn delete_marks_source_and_responds_202() {
    let (pool, db_name, router) = spawn_env("sources_delete").await;

    create_source(router.clone(), "client_a").await;

    // DELETE только помечает: 202 без тела, исполнение — за воркером.
    let (status, body) = send_json(
        router.clone(),
        "DELETE",
        "/api/sources/client_a",
        json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.is_null(), "тело 202 пустое: {body}");

    // Строка реестра на месте, помечена к удалению.
    let row = sqlx::query("SELECT deleted_at, updated_at FROM sources WHERE name = $1")
        .bind("client_a")
        .fetch_one(&pool)
        .await
        .expect("строка осталась");
    let deleted_at = row
        .get::<Option<chrono::DateTime<chrono::Utc>>, _>("deleted_at")
        .expect("помечен к удалению");

    // Повторный DELETE идемпотентен: 202, дата пометки не перетирается.
    let (status, _) = send_json(
        router.clone(),
        "DELETE",
        "/api/sources/client_a",
        json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let row = sqlx::query("SELECT deleted_at FROM sources WHERE name = $1")
        .bind("client_a")
        .fetch_one(&pool)
        .await
        .expect("строка осталась");
    assert_eq!(
        row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("deleted_at"),
        Some(deleted_at),
        "повторная пометка не сдвигает дату"
    );

    // Заметно в API: источник виден в списке с заполненной deleted_at.
    let (status, body) = get_json(router.clone(), "/api/sources").await;
    assert_eq!(status, StatusCode::OK);
    let list = body.as_array().expect("sources");
    assert_eq!(list.len(), 1);
    assert!(list[0]["deleted_at"].is_string());

    // Чтение и удаление неизвестного — 404.
    let (status, _) = get_json(router.clone(), "/api/sources/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send_json(router, "DELETE", "/api/sources/nope", json!(null)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}

#[tokio::test]
async fn patch_after_delete_conflicts() {
    let (_pool, db_name, router) = spawn_env("sources_patch_deleted").await;

    create_source(router.clone(), "client_a").await;
    let (status, _) = send_json(
        router.clone(),
        "DELETE",
        "/api/sources/client_a",
        json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // Правка помеченного к удалению — конфликт состояния, а не правка.
    let (status, body) = send_json(
        router.clone(),
        "PATCH",
        "/api/sources/client_a",
        json!({ "batch_size": 100 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "правка помеченного: {body}");

    // Чтение помеченного работает: статус удаления виден оператору.
    let (status, body) = get_json(router, "/api/sources/client_a").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["deleted_at"].is_string());

    drop_db(&connect_admin(&audit_admin_url()).await, &db_name).await;
}
