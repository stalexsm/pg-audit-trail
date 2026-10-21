//! Интеграционный тест шва №1 «живые два Postgres» (эпик #3, тикеты #6 и #8).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d` (db1 и db-src
//! с проброшенными портами). Воркер запускается из библиотечной части
//! поверх уникальных БД: на источнике эмитится нагрузка чистым SQL —
//! I/U/D/T, MESSAGE (`_audit` и чужие префиксы, несколько в одной
//! транзакции) и вставки в игнорируемые таблицы; ассерты — по
//! содержимому журнала `changes` в БД аудита.

mod common;

use std::time::Duration;

use common::{
    audit_admin_url, cleanup_leftovers, connect_db, drop_db, setup_source, source_admin_url,
    spawn_db, wait_until,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use worker_cdc::WorkerConfig;

/// Подключение к администраторскому пулу стенда (db1 / db-src).
async fn connect_admin(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите `docker compose up -d`")
}

#[tokio::test]
async fn live_replication_ingests_changes_to_journal() {
    let audit_admin = connect_admin(&audit_admin_url()).await;
    let source_admin = connect_admin(&source_admin_url()).await;

    // Остатки упавших прогонов (БД + их слоты) мешают новому прогону.
    // Префиксы этого сценария: параллельные тесты не задевают друг друга.
    cleanup_leftovers(&audit_admin, "waltrail_audit_%").await;
    cleanup_leftovers(&source_admin, "waltrail_src_%").await;

    let (audit_url, audit_name) =
        spawn_db(&audit_admin, &audit_admin_url(), "waltrail_audit").await;
    let (source_url, source_name) =
        spawn_db(&source_admin, &source_admin_url(), "waltrail_src").await;

    // Слот уникален на прогон: остатки прошлых запусков не должны мешать.
    let slot_name = format!("waltrail_test_{}", uuid::Uuid::new_v4().simple());

    let source = connect_db(&source_url).await;
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
        reconnect_initial_backoff_ms: 1_000,
        reconnect_max_backoff_ms: 30_000,
    };

    let token = tokio_util::sync::CancellationToken::new();
    let worker_token = token.clone();
    let handle = tokio::spawn(worker_cdc::run(config, worker_token));

    // Пул к журналу аудита: воркер применит миграции, таблица появится чуть позже.
    let audit = connect_db(&audit_url).await;

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

    // Транзакция 1: два MESSAGE чужого префикса (съедают event_index 0 и 1,
    // строк в журнале не создают) + INSERT.
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

    // Игнорируемые таблицы и MESSAGE чужих префиксов в журнал не попадают:
    // до этого места MESSAGE с префиксом `_audit` ещё не эмитились вовсе.
    let ignored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM changes \
         WHERE \"table\" = 'schema_migrations' OR operation = 'MESSAGE'",
    )
    .fetch_one(&audit)
    .await
    .expect("подсчёт игнорируемых строк");
    assert_eq!(
        ignored, 0,
        "служебные таблицы и MESSAGE без префикса _audit не должны попадать в журнал"
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

    // --- MESSAGE-события приложения (ADR-0002, тикет #8) ------------------

    // AC #1: INSERT + emit_message — строка изменения с контекстом и
    // trace_id плюс строка события.
    let mut tx = source.begin().await.expect("транзакция message-insert");
    sqlx::query("INSERT INTO orders VALUES (3, 7.25, 'с сообщением', now())")
        .execute(&mut *tx)
        .await
        .expect("insert orders 3");
    sqlx::query(
        "SELECT pg_logical_emit_message(true, '_audit', \
         '{\"trace_id\": \"op-3\", \"user\": \"alice\"}')",
    )
    .execute(&mut *tx)
    .await
    .expect("emit_message _audit op-3");
    tx.commit().await.expect("коммит message-insert");

    // AC #2: событие без изменений строк.
    sqlx::query(
        "SELECT pg_logical_emit_message(true, '_audit', \
         '{\"trace_id\": \"op-4\", \"action\": \"export\"}')",
    )
    .execute(&source)
    .await
    .expect("emit_message _audit op-4");

    // AC #3: два MESSAGE в одной транзакции + строка изменений.
    let mut tx = source.begin().await.expect("транзакция two-messages");
    sqlx::query("SELECT pg_logical_emit_message(true, '_audit', '{\"trace_id\": \"op-5a\"}')")
        .execute(&mut *tx)
        .await
        .expect("emit_message op-5a");
    sqlx::query(
        "SELECT pg_logical_emit_message(true, '_audit', \
         '{\"trace_id\": \"op-5b\", \"step\": 2}')",
    )
    .execute(&mut *tx)
    .await
    .expect("emit_message op-5b");
    sqlx::query("UPDATE orders SET note = 'после сообщений' WHERE id = 3")
        .execute(&mut *tx)
        .await
        .expect("update orders 3");
    tx.commit().await.expect("коммит two-messages");

    // AC #4: MESSAGE без trace_id; заодно не-JSON payload — поток не
    // должен сломаться, строка журнала для него не создаётся.
    sqlx::query("SELECT pg_logical_emit_message(true, '_audit', '{\"user\": \"bob\"}')")
        .execute(&source)
        .await
        .expect("emit_message _audit без trace_id");
    sqlx::query("SELECT pg_logical_emit_message(true, '_audit', 'не-json')")
        .execute(&source)
        .await
        .expect("emit_message _audit не-json");

    // 10 строк изменений (orders 6, events 2, composite 2) + 5 MESSAGE
    // (op-3, op-4, op-5a, op-5b и без trace_id; не-JSON — мимо). Ждём
    // суммарно: транзакции без изменений строк не видны в подсчёте
    // таблиц, журнал должен догрузиться целиком.
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM changes \
             WHERE operation = 'MESSAGE' OR \"table\" IN ('orders', 'events', 'composite')",
        )
        .fetch_one(&audit)
        .await
        .unwrap_or(0)
            == 15
    })
    .await;
    let messages: i64 =
        sqlx::query_scalar("SELECT count(*) FROM changes WHERE operation = 'MESSAGE'")
            .fetch_one(&audit)
            .await
            .expect("подсчёт MESSAGE-строк");
    assert_eq!(messages, 5, "в журнал попадают только _audit MESSAGE");

    // AC #1: строка изменения получила payload дословно и trace_id в колонку.
    let order3_create = sqlx::query(
        "SELECT context, trace_id FROM changes \
         WHERE \"table\" = 'orders' AND operation = 'CREATE' AND after->>'id' = '3'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка CREATE orders 3");
    let context: serde_json::Value = order3_create.get("context");
    assert_eq!(
        context,
        serde_json::json!({"trace_id": "op-3", "user": "alice"}),
        "payload MESSAGE сохраняется в контексте строки изменения дословно"
    );
    assert_eq!(
        order3_create
            .get::<Option<String>, _>("trace_id")
            .as_deref(),
        Some("op-3")
    );

    // MESSAGE-строка: payload и trace_id, пустые привязки к таблице.
    let op3_event = sqlx::query(
        "SELECT context, trace_id, schema, \"table\", primary_key, before, after \
         FROM changes WHERE operation = 'MESSAGE' AND context->>'trace_id' = 'op-3'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка MESSAGE op-3");
    let context: serde_json::Value = op3_event.get("context");
    assert_eq!(
        context,
        serde_json::json!({"trace_id": "op-3", "user": "alice"})
    );
    assert_eq!(
        op3_event.get::<Option<String>, _>("trace_id").as_deref(),
        Some("op-3")
    );
    assert_eq!(op3_event.get::<String, _>("schema"), "");
    assert_eq!(op3_event.get::<String, _>("table"), "");
    assert_eq!(op3_event.get::<Option<String>, _>("primary_key"), None);
    assert_eq!(
        op3_event.get::<serde_json::Value, _>("before"),
        serde_json::json!({})
    );
    assert_eq!(
        op3_event.get::<serde_json::Value, _>("after"),
        serde_json::json!({})
    );

    // AC #2: событие без изменений строк — строкой MESSAGE.
    let op4_event = sqlx::query(
        "SELECT trace_id FROM changes \
         WHERE operation = 'MESSAGE' AND context->>'action' = 'export'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка MESSAGE op-4");
    assert_eq!(
        op4_event.get::<Option<String>, _>("trace_id").as_deref(),
        Some("op-4")
    );

    // AC #3: две строки событий; строке изменений — контекст последнего.
    let update3 = sqlx::query(
        "SELECT context, trace_id FROM changes \
         WHERE \"table\" = 'orders' AND operation = 'UPDATE' AND after->>'id' = '3'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка UPDATE orders 3");
    let context: serde_json::Value = update3.get("context");
    assert_eq!(context, serde_json::json!({"trace_id": "op-5b", "step": 2}));
    assert_eq!(
        update3.get::<Option<String>, _>("trace_id").as_deref(),
        Some("op-5b")
    );
    let both: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM changes WHERE operation = 'MESSAGE' \
         AND context->>'trace_id' IN ('op-5a', 'op-5b')",
    )
    .fetch_one(&audit)
    .await
    .expect("две строки MESSAGE одной транзакции");
    assert_eq!(both, 2, "каждый MESSAGE транзакции — своя строка");

    // AC #4: без trace_id — колонка пустая, payload дословно.
    let bob = sqlx::query(
        "SELECT context, trace_id FROM changes \
         WHERE operation = 'MESSAGE' AND context->>'user' = 'bob'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка MESSAGE без trace_id");
    assert_eq!(bob.get::<Option<String>, _>("trace_id"), None);
    let context: serde_json::Value = bob.get("context");
    assert_eq!(context, serde_json::json!({"user": "bob"}));

    // Чекпоинт догнал MESSAGE-транзакции.
    let final_checkpoint: i64 =
        sqlx::query_scalar("SELECT last_commit_lsn FROM checkpoints WHERE name = $1")
            .bind(&slot_name)
            .fetch_one(&audit)
            .await
            .expect("чекпоинт после MESSAGE-транзакций");
    assert!(
        final_checkpoint >= checkpoint,
        "чекпоинт не откатывается назад"
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
