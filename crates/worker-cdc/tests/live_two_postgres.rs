//! Интеграционный тест шва №1 «живые два Postgres» (эпик #3, тикет #6).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d` (db1 и db-src
//! с проброшенными портами). Воркер запускается из библиотечной части
//! поверх уникальных БД: на источнике эмитится нагрузка чистым SQL —
//! I/U/D/T, MESSAGE и вставки в игнорируемые таблицы; ассерты — по
//! содержимому журнала `changes` в БД аудита.

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use worker_cdc::WorkerConfig;

/// Подключение к тестовой БД аудита (db1).
fn audit_admin_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:54399/postgres".to_string())
}

/// Подключение к тестовому источнику CDC (db-src).
fn source_admin_url() -> String {
    std::env::var("TEST_SOURCE_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:54398/postgres".to_string())
}

/// Гигиена стенда: сносит тестовые БД прошлых прогонов (вместе с их слотами —
/// DROP DATABASE FORCE на PG 16+ удаляет и логические слоты). Незакрытые
/// после падения прогонов слоты, привязанные к чужим БД, блокировали бы
/// START_REPLICATION нового прогона.
async fn cleanup_leftovers(admin: &PgPool, pattern: &str) {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT datname FROM pg_database WHERE datname LIKE $1 AND datname != current_database()",
    )
    .bind(pattern)
    .fetch_all(admin)
    .await
    .unwrap_or_default();
    for name in names {
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(admin)
            .await;
    }
}

/// Создаёт уникальную БД на администраторском пуле, возвращает её URL.
async fn spawn_db(admin: &PgPool, base_url: &str, prefix: &str) -> (String, String) {
    let db_name = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(admin)
        .await
        .expect("создание тестовой БД");

    let (prefix_url, _) = base_url
        .rsplit_once('/')
        .expect("URL должен содержать путь к БД");
    let url = format!("{prefix_url}/{db_name}");
    (url, db_name)
}

async fn drop_db(admin: &PgPool, db_name: &str) {
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
    ))
    .execute(admin)
    .await
    .expect("удаление тестовой БД");
}

/// Готовит источник: публикация `audit` на все таблицы и тестовые таблицы.
async fn setup_source(source: &PgPool) {
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
async fn wait_until<F, Fut>(mut predicate: F)
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

#[tokio::test]
async fn live_replication_ingests_changes_to_journal() {
    let audit_admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&audit_admin_url())
        .await
        .expect("нет подключения к db1 — поднимите `docker compose up -d`");
    let source_admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&source_admin_url())
        .await
        .expect("нет подключения к db-src — поднимите `docker compose up -d`");

    // Остатки упавших прогонов (БД + их слоты) мешают новому прогону.
    cleanup_leftovers(&audit_admin, "waltrail_audit_%").await;
    cleanup_leftovers(&source_admin, "waltrail_src_%").await;

    let (audit_url, audit_name) =
        spawn_db(&audit_admin, &audit_admin_url(), "waltrail_audit").await;
    let (source_url, source_name) =
        spawn_db(&source_admin, &source_admin_url(), "waltrail_src").await;

    // Слот уникален на прогон: остатки прошлых запусков не должны мешать.
    let slot_name = format!("waltrail_test_{}", uuid::Uuid::new_v4().simple());

    let source = PgPoolOptions::new()
        .max_connections(3)
        .connect(&source_url)
        .await
        .expect("подключение к уникальному источнику");
    setup_source(&source).await;

    let config = WorkerConfig {
        database_url: audit_url.clone(),
        source_database_url: source_url.clone(),
        slot: slot_name.clone(),
        publication: "audit".to_owned(),
        ignore_tables: vec!["public.schema_migrations".to_owned()],
        // Маленький чанк, чтобы батчевая вставка разбивалась на несколько
        // push_values внутри одной транзакции.
        batch_size: 2,
    };

    let token = tokio_util::sync::CancellationToken::new();
    let worker_token = token.clone();
    let handle = tokio::spawn(worker_cdc::run(config, worker_token));

    // Пул к журналу аудита: воркер применит миграции, таблица появится чуть позже.
    let audit = PgPoolOptions::new()
        .max_connections(2)
        .connect(&audit_url)
        .await
        .expect("подключение к уникальной БД аудита");

    // Бутстрап воркера обязан создать слот, раз его нет.
    let slot_check = slot_name.clone();
    wait_until(|| async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
        )
        .bind(&slot_check)
        .fetch_one(&source)
        .await
        .unwrap_or(false)
    })
    .await;

    // --- SQL-эмиттер ------------------------------------------------------

    // Транзакция 1: два MESSAGE (съедают event_index 0 и 1) + INSERT.
    let mut tx = source.begin().await.expect("транзакция 1");
    sqlx::query("SELECT pg_logical_emit_message(true, 'waltrail', '{\"trace_id\":\"op-1\"}')")
        .execute(&mut *tx)
        .await
        .expect("emit_message");
    sqlx::query("SELECT pg_logical_emit_message(true, 'waltrail', '{\"trace_id\":\"op-1b\"}')")
        .execute(&mut *tx)
        .await
        .expect("emit_message 2");
    sqlx::query("INSERT INTO orders VALUES (1, 10.50, 'первый', now())")
        .execute(&mut *tx)
        .await
        .expect("insert orders 1");
    tx.commit().await.expect("коммит 1");

    // Транзакция 2: UPDATE с полным образом «до» (REPLICA IDENTITY FULL).
    sqlx::query("UPDATE orders SET note = 'второй', amount = 20.25 WHERE id = 1")
        .execute(&source)
        .await
        .expect("update orders 1");

    // Транзакция 3: INSERT и DELETE одной транзакции.
    let mut tx = source.begin().await.expect("транзакция 3");
    sqlx::query("INSERT INTO orders VALUES (2, 5.00, 'третий', now())")
        .execute(&mut *tx)
        .await
        .expect("insert orders 2");
    sqlx::query("DELETE FROM orders WHERE id = 2")
        .execute(&mut *tx)
        .await
        .expect("delete orders 2");
    tx.commit().await.expect("коммит 3");

    // Транзакция 4: INSERT + TRUNCATE.
    let mut tx = source.begin().await.expect("транзакция 4");
    sqlx::query("INSERT INTO events VALUES (1, 'go')")
        .execute(&mut *tx)
        .await
        .expect("insert events 1");
    sqlx::query("TRUNCATE events")
        .execute(&mut *tx)
        .await
        .expect("truncate events");
    tx.commit().await.expect("коммит 4");

    // Транзакция 5: игнорируемая (служебная) таблица.
    sqlx::query("INSERT INTO schema_migrations VALUES (1, 'ignored')")
        .execute(&source)
        .await
        .expect("insert schema_migrations");

    // Составной ключ: INSERT с ключом (7, 8).
    sqlx::query("INSERT INTO composite VALUES (7, 8, 'составной')")
        .execute(&source)
        .await
        .expect("insert composite");

    // Внетранзакционный MESSAGE не должен ломать поток.
    sqlx::query("SELECT pg_logical_emit_message(false, 'waltrail', '{\"trace_id\":\"op-0\"}')")
        .execute(&source)
        .await
        .expect("emit_message non-tx");

    // --- Ассерты по журналу ----------------------------------------------

    // Ожидается 7 строк: 4 по orders, CREATE+TRUNCATE по events, 1 по composite.
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM changes WHERE \"table\" IN ('orders', 'events', 'composite')",
        )
        .fetch_one(&audit)
        .await
        .unwrap_or(0)
            == 7
    })
    .await;

    // Игнорируемые таблицы и MESSAGE-события в журнал не попадают.
    let ignored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM changes \
         WHERE \"table\" = 'schema_migrations' OR operation = 'MESSAGE'",
    )
    .fetch_one(&audit)
    .await
    .expect("подсчёт игнорируемых строк");
    assert_eq!(
        ignored, 0,
        "служебные таблицы и MESSAGE не должны попадать в журнал"
    );

    // Строка INSERT orders: полный образ «после», ключ, commit_lsn/event_index.
    let insert_row = sqlx::query(
        "SELECT primary_key, before, after, committed_at, queued_at, transaction_id, \
                position, commit_lsn, event_index, created_at \
         FROM changes WHERE \"table\" = 'orders' AND operation = 'CREATE' AND after->>'id' = '1'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка INSERT orders в журнале");

    assert_eq!(
        insert_row.get::<Option<String>, _>("primary_key"),
        Some("1".to_owned()),
        "primary_key из key-флагов Relation должен заполняться и для INSERT"
    );
    let after: serde_json::Value = insert_row.get("after");
    assert_eq!(after["id"], 1, "после: id числом (int8)");
    assert_eq!(
        after["amount"], "10.50",
        "после: numeric строкой без потери точности"
    );
    assert_eq!(after["note"], "первый");
    assert!(
        after["created_at"].as_str().is_some_and(|s| !s.is_empty()),
        "после: timestamptz в текстовом представлении"
    );
    let before: serde_json::Value = insert_row.get("before");
    assert_eq!(before, serde_json::json!({}), "до: пустой образ у INSERT");

    // MESSAGE в транзакции 1 расходуют event_index 0 и 1: у INSERT остаётся 2.
    assert_eq!(insert_row.get::<i32, _>("event_index"), 2);
    let position: i64 = insert_row.get("position");
    let commit_lsn: i64 = insert_row.get("commit_lsn");
    assert!(position > 0, "position = LSN события");
    assert!(commit_lsn > 0, "commit_lsn = LSN коммита источника");
    assert!(
        position < commit_lsn,
        "изменение в WAL строго раньше своего коммита"
    );
    assert!(insert_row.get::<i64, _>("transaction_id") > 0);

    let committed_at: chrono::DateTime<chrono::Utc> = insert_row.get("committed_at");
    assert!(
        (chrono::Utc::now() - committed_at).num_minutes() < 5,
        "committed_at — время коммита источника: {committed_at}"
    );

    // Строка UPDATE: полный образ «до» при REPLICA IDENTITY FULL.
    let update_row = sqlx::query(
        "SELECT primary_key, before, after, position, commit_lsn, event_index \
         FROM changes WHERE \"table\" = 'orders' AND operation = 'UPDATE'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка UPDATE orders в журнале");
    let before: serde_json::Value = update_row.get("before");
    assert_eq!(before["id"], 1);
    assert_eq!(
        before["amount"], "10.50",
        "до: старые значения полным образом"
    );
    assert_eq!(before["note"], "первый");
    let after: serde_json::Value = update_row.get("after");
    assert_eq!(after["amount"], "20.25");
    assert_eq!(after["note"], "второй");
    assert_eq!(
        update_row.get::<Option<String>, _>("primary_key"),
        Some("1".to_owned())
    );
    assert_eq!(update_row.get::<i32, _>("event_index"), 0);

    // Транзакция 3: CREATE и DELETE с одним commit_lsn и разными позициями.
    // Сортировка по (commit_lsn, event_index) — порядку коммитов в WAL.
    let tx3_rows = sqlx::query(
        "SELECT operation, primary_key, before, after, position, event_index, commit_lsn \
         FROM changes WHERE \"table\" = 'orders' ORDER BY commit_lsn, event_index",
    )
    .fetch_all(&audit)
    .await
    .expect("строки orders в журнале");
    assert_eq!(tx3_rows.len(), 4, "orders: CREATE, UPDATE, CREATE, DELETE");

    let create2 = &tx3_rows[2];
    let delete2 = &tx3_rows[3];
    assert_eq!(create2.get::<&str, _>("operation"), "CREATE");
    assert_eq!(delete2.get::<&str, _>("operation"), "DELETE");
    assert_eq!(
        create2.get::<i64, _>("commit_lsn"),
        delete2.get::<i64, _>("commit_lsn"),
        "одна транзакция источника — один commit_lsn"
    );
    assert_eq!(create2.get::<i32, _>("event_index"), 0);
    assert_eq!(delete2.get::<i32, _>("event_index"), 1);
    assert_ne!(
        create2.get::<i64, _>("position"),
        delete2.get::<i64, _>("position"),
        "у каждого события свой LSN"
    );
    assert_eq!(
        delete2.get::<Option<String>, _>("primary_key"),
        Some("2".to_owned()),
        "primary_key заполняется и для DELETE"
    );
    let delete_before: serde_json::Value = delete2.get("before");
    assert_eq!(delete_before["note"], "третий", "до: полный образ при FULL");
    let delete_after: serde_json::Value = delete2.get("after");
    assert_eq!(delete_after, serde_json::json!({}));

    // events: CREATE с ключом из флагов (identity DEFAULT) и TRUNCATE без ключа.
    let event_create = sqlx::query(
        "SELECT primary_key, before, after, position FROM changes \
         WHERE \"table\" = 'events' AND operation = 'CREATE'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка CREATE events");
    assert_eq!(
        event_create.get::<Option<String>, _>("primary_key"),
        Some("1".to_owned()),
        "ключ из key-флагов работает и при identity DEFAULT"
    );
    let event_truncate = sqlx::query(
        "SELECT primary_key, before, after FROM changes \
         WHERE \"table\" = 'events' AND operation = 'TRUNCATE'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка TRUNCATE events");
    assert_eq!(event_truncate.get::<Option<String>, _>("primary_key"), None);
    assert_eq!(
        event_truncate.get::<serde_json::Value, _>("before"),
        serde_json::json!({})
    );
    assert_eq!(
        event_truncate.get::<serde_json::Value, _>("after"),
        serde_json::json!({})
    );

    // Составной ключ кодируется JSON-массивом.
    let composite_pk: Option<String> =
        sqlx::query_scalar("SELECT primary_key FROM changes WHERE \"table\" = 'composite'")
            .fetch_one(&audit)
            .await
            .expect("строка composite");
    assert_eq!(composite_pk.as_deref(), Some("[\"7\",\"8\"]"));

    // ALTER TABLE на источнике: повторный Relation должен перезаписать кэш —
    // новая колонка появляется в образе «после» без рестарта воркера.
    sqlx::query("ALTER TABLE composite ADD COLUMN extra TEXT")
        .execute(&source)
        .await
        .expect("alter table composite");
    sqlx::query("INSERT INTO composite VALUES (9, 9, 'после-alter', 'доп')")
        .execute(&source)
        .await
        .expect("insert composite после alter");

    // Ожидается 8-я строка: новый INSERT в composite уже с новой колонкой.
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM changes WHERE \"table\" IN ('orders', 'events', 'composite')",
        )
        .fetch_one(&audit)
        .await
        .unwrap_or(0)
            == 8
    })
    .await;

    let after_alter = sqlx::query(
        "SELECT primary_key, after FROM changes \
         WHERE \"table\" = 'composite' AND after->>'a' = '9'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка composite после ALTER TABLE");
    let after: serde_json::Value = after_alter.get("after");
    assert_eq!(
        after["extra"], "доп",
        "новая колонка после ALTER TABLE в образе"
    );
    assert_eq!(
        after_alter
            .get::<Option<String>, _>("primary_key")
            .as_deref(),
        Some("[\"9\",\"9\"]"),
        "ключ после ALTER TABLE собирается заново"
    );

    // Чекпоинт зафиксирован и не заходит за последний закоммиченный LSN.
    let checkpoint: i64 =
        sqlx::query_scalar("SELECT last_commit_lsn FROM checkpoints WHERE name = $1")
            .bind(&slot_name)
            .fetch_one(&audit)
            .await
            .expect("чекпоинт после инжеста");
    assert!(
        checkpoint > commit_lsn,
        "чекпоинт = LSN конца последней транзакции"
    );

    // Graceful shutdown по CancellationToken: воркер завершается без ошибки.
    token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("воркер не завершился за 30 секунд после отмены")
        .expect("таска воркера не запаниковала");
    assert!(
        result.is_ok(),
        "воркер должен завершиться штатно: {result:?}"
    );

    drop_db(&audit_admin, &audit_name).await;
    drop_db(&source_admin, &source_name).await;
}
