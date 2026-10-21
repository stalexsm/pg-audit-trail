//! Интеграционный тест шва «выживание воркера» (эпик #3, тикет #7).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d`. Воркер
//! запускается из библиотечной части поверх уникальных БД; сценарии
//! из критериев приёмки: жёсткий kill на середине потока и рестарт,
//! принудительный обрыв walsender (`pg_terminate_backend`) с реконнектом
//! и backoff, длинная пауза эмиттера против `wal_sender_timeout`
//! (keepalive), SIGTERM с рестартом. Истина — журнал `changes`: каждое
//! событие ровно один раз, без потерь и дублей.

mod common;

use std::sync::Once;
use std::time::Duration;

use common::{
    audit_admin_url, cleanup_leftovers, connect_db, drop_db, journal_duplicate_pairs, setup_source,
    source_admin_url, spawn_db, wait_until,
};
use sqlx::PgPool;
use worker_cdc::WorkerConfig;

/// Логи воркера внутри теста: без подписчика warn-и реконнектов не видны.
/// Вызывать в начале каждого теста; вывод попадает в захват cargo test.
static INIT_LOGGING: Once = Once::new();
fn init_logging() {
    INIT_LOGGING.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("worker_cdc=info")),
            )
            .with_target(false)
            .with_test_writer()
            .init();
    });
}

/// Подключение к администраторскому пулу стенда (db1 / db-src).
async fn connect_admin(url: &str) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите `docker compose up -d`")
}

/// Конфиг воркера под сценарий выживания: уникальные БД и слот,
/// маленький чанк батча, быстрый backoff для коротких тестов.
fn worker_config(
    audit_url: String,
    source_url: String,
    slot: String,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
) -> WorkerConfig {
    WorkerConfig {
        database_url: audit_url,
        source_database_url: source_url,
        slot,
        publication: "audit".to_owned(),
        ignore_tables: vec!["public.schema_migrations".to_owned()],
        batch_size: 2,
        reconnect_initial_backoff_ms: initial_backoff_ms,
        reconnect_max_backoff_ms: max_backoff_ms,
    }
}

/// Поднимает пару уникальных БД (аудит + источник) под префикс сценария
/// и готовит источник: публикация и тестовые таблицы. Чистка остатков
/// идёт по тому же префиксу, поэтому параллельные сценарии изолированы.
async fn spawn_env(prefix: &str) -> (PgPool, PgPool, String, String, String, String) {
    let audit_admin = connect_admin(&audit_admin_url()).await;
    let source_admin = connect_admin(&source_admin_url()).await;

    let audit_pattern = format!("waltrail_audit_{prefix}_%");
    let source_pattern = format!("waltrail_src_{prefix}_%");
    cleanup_leftovers(&audit_admin, &audit_pattern).await;
    cleanup_leftovers(&source_admin, &source_pattern).await;

    let (audit_url, audit_name) = spawn_db(
        &audit_admin,
        &audit_admin_url(),
        &format!("waltrail_audit_{prefix}"),
    )
    .await;
    let (source_url, source_name) = spawn_db(
        &source_admin,
        &source_admin_url(),
        &format!("waltrail_src_{prefix}"),
    )
    .await;

    let source = connect_db(&source_url).await;
    setup_source(&source).await;

    (
        audit_admin,
        source_admin,
        audit_url,
        source_url,
        audit_name,
        source_name,
    )
}

/// application_name walsender-соединения воркера — формат из `replicate.rs`.
fn application_name(slot: &str) -> String {
    format!("waltrail-cdc-{slot}")
}

/// Короткий уникальный суффикс имени слота: сервер обрезает
/// application_name до 63 байт (NAMEDATALEN), поэтому полный UUID не
/// влезает и соединение не находится в pg_stat_replication точным совпадением.
fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_owned()
}

/// PID активного walsender-бэкенда слота в `pg_stat_replication`
/// (нет живого соединения — `None`).
async fn walsender_pid(source_admin: &PgPool, app_name: &str) -> Option<i32> {
    sqlx::query_scalar("SELECT pid FROM pg_stat_replication WHERE application_name = $1")
        .bind(app_name)
        .fetch_optional(source_admin)
        .await
        .unwrap_or(None)
}

/// Ждёт, пока воркер поднимет схему аудита, создаст слот и подключится
/// к репликации; возвращает PID его walsender-соединения.
async fn wait_worker_ready(
    audit: &PgPool,
    source: &PgPool,
    source_admin: &PgPool,
    slot: &str,
) -> i32 {
    // Воркер применяет миграции после старта: сначала ждём таблицу журнала.
    wait_until(|| async {
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('public.changes') IS NOT NULL")
            .fetch_one(audit)
            .await
            .unwrap_or(false)
    })
    .await;

    let slot_name = slot.to_owned();
    wait_until(|| async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
        )
        .bind(&slot_name)
        .fetch_one(source)
        .await
        .unwrap_or(false)
    })
    .await;

    let app_name = application_name(slot);
    wait_until(|| async { walsender_pid(source_admin, &app_name).await.is_some() }).await;

    walsender_pid(source_admin, &app_name)
        .await
        .expect("walsender-соединение воркера")
}

/// Вставляет строку в orders на источнике (одно событие CREATE в журнале).
async fn emit_order(source: &PgPool, id: i64, note: &str) {
    sqlx::query("INSERT INTO orders VALUES ($1, 1.00, $2, now())")
        .bind(id)
        .bind(note)
        .execute(source)
        .await
        .expect("insert orders");
}

/// Ждёт, пока в журнале по orders появится `expected` строк.
async fn wait_journal_count(audit: &PgPool, expected: i64) {
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM changes WHERE \"table\" = 'orders'")
            .fetch_one(audit)
            .await
            .unwrap_or(0)
            == expected
    })
    .await;
}

/// Штатное завершение воркера после отмены: без паники и ошибки.
async fn shutdown_cleanly(
    token: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("воркер не завершился за 30 секунд после отмены")
        .expect("таска воркера не запаниковала");
    assert!(
        result.is_ok(),
        "воркер должен завершиться штатно: {result:?}"
    );
}

/// AC #1: воркер глушат на середине потока и поднимают снова — все события
/// в журнале ровно по одному разу (жёсткий abort без graceful-процедуры).
#[tokio::test]
async fn hard_kill_and_restart_delivers_exactly_once() {
    init_logging();
    let (audit_admin, source_admin, audit_url, source_url, audit_name, source_name) =
        spawn_env("killrun").await;
    let source = connect_db(&source_url).await;
    let audit = connect_db(&audit_url).await;
    let slot_name = format!("waltrail_kill_{}", short_id());

    let config = worker_config(audit_url, source_url, slot_name.clone(), 1_000, 30_000);
    let token = tokio_util::sync::CancellationToken::new();
    let handle = tokio::spawn(worker_cdc::run(config.clone(), token.clone()));
    wait_worker_ready(&audit, &source, &source_admin, &slot_name).await;

    // Первая половина потока до глушения.
    for id in 1..=3 {
        emit_order(&source, id, "до-убийства").await;
    }
    wait_journal_count(&audit, 3).await;

    // Глушим с событием «в полёте»: оно уже в WAL источника, но воркер не
    // обязан был его сохранить — abort попадает в случайную точку обработки
    // (буфер транзакции, вставка или ack слота). Короткая пауза даёт воркеру
    // шанс забрать событие из потока до глушения.
    emit_order(&source, 4, "в полёте").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Глушим мгновенно, без graceful-процедуры: как SIGKILL.
    handle.abort();
    let _ = handle.await;

    // Остальное пишется в отсутствие воркера.
    for id in 5..=6 {
        emit_order(&source, id, "после-убийства").await;
    }

    // Поднимаем снова: тот же слот и чекпоинт, хвост потока доигрывается.
    let token2 = tokio_util::sync::CancellationToken::new();
    let handle2 = tokio::spawn(worker_cdc::run(config, token2.clone()));
    wait_journal_count(&audit, 6).await;

    // Ровно по одному разу: нет дублирующихся пар и каждая строка одна.
    assert_eq!(
        journal_duplicate_pairs(&audit).await,
        0,
        "replay не должен дублировать события журнала"
    );
    let distinct_ids: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT after->>'id') FROM changes WHERE \"table\" = 'orders'",
    )
    .fetch_one(&audit)
    .await
    .expect("подсчёт уникальных строк orders");
    assert_eq!(
        distinct_ids, 6,
        "каждая строка источника — ровно одна запись"
    );

    shutdown_cleanly(token2, handle2).await;
    drop_db(&audit_admin, &audit_name).await;
    drop_db(&source_admin, &source_name).await;
}

/// AC #3: принудительный обрыв соединения (pg_terminate_backend walsender)
/// → реконнект с backoff, продолжение с чекпоинта без потерь и дублей.
#[tokio::test]
async fn forced_disconnect_reconnects_and_continues() {
    init_logging();
    let (audit_admin, source_admin, audit_url, source_url, audit_name, source_name) =
        spawn_env("reconnect").await;
    let source = connect_db(&source_url).await;
    let audit = connect_db(&audit_url).await;
    let slot_name = format!("waltrail_reconnect_{}", short_id());

    // Быстрый backoff: реконнект не должен растянуть тест.
    let config = worker_config(audit_url, source_url, slot_name.clone(), 200, 2_000);
    let token = tokio_util::sync::CancellationToken::new();
    let handle = tokio::spawn(worker_cdc::run(config, token.clone()));

    let app_name = application_name(&slot_name);
    let first_pid = wait_worker_ready(&audit, &source, &source_admin, &slot_name).await;

    emit_order(&source, 1, "до-обрыва").await;
    wait_journal_count(&audit, 1).await;

    // Принудительный обрыв: сервер прибивает walsender-бэкенд воркера.
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(first_pid)
        .execute(&source_admin)
        .await
        .expect("pg_terminate_backend");

    // Соединение поднимается заново — уже с другим PID.
    wait_until(|| async {
        matches!(
            walsender_pid(&source_admin, &app_name).await,
            Some(pid) if pid != first_pid
        )
    })
    .await;

    // Поток продолжается с чекпоинта: событие после обрыва доезжает.
    emit_order(&source, 2, "после-обрыва").await;
    wait_journal_count(&audit, 2).await;

    assert_eq!(
        journal_duplicate_pairs(&audit).await,
        0,
        "после реконнекта дубликатов быть не должно"
    );
    assert!(
        !handle.is_finished(),
        "воркер обязан пережить обрыв, не завершаясь"
    );

    shutdown_cleanly(token, handle).await;
    drop_db(&audit_admin, &audit_name).await;
    drop_db(&source_admin, &source_name).await;
}

/// AC #2: долгая пауза эмиттера не рвёт соединение. Пауза больше дефолтного
/// `wal_sender_timeout` (60 c): без ответов на keepalive и статусных апдейтов
/// сервер оборвал бы walsender и PID соединения сменился бы.
#[tokio::test]
async fn idle_pause_does_not_break_connection() {
    init_logging();
    let (audit_admin, source_admin, audit_url, source_url, audit_name, source_name) =
        spawn_env("pause").await;
    let source = connect_db(&source_url).await;
    let audit = connect_db(&audit_url).await;
    let slot_name = format!("waltrail_pause_{}", short_id());

    let config = worker_config(audit_url, source_url, slot_name.clone(), 1_000, 30_000);
    let token = tokio_util::sync::CancellationToken::new();
    let handle = tokio::spawn(worker_cdc::run(config, token.clone()));

    let app_name = application_name(&slot_name);
    let first_pid = wait_worker_ready(&audit, &source, &source_admin, &slot_name).await;

    // Долгая тишина эмиттера: дольше wal_sender_timeout по умолчанию (60 c).
    tokio::time::sleep(Duration::from_secs(70)).await;

    let alive_pid = walsender_pid(&source_admin, &app_name)
        .await
        .unwrap_or_else(|| {
            panic!("walsender-соединение потеряно за паузу: keepalive не подтверждён")
        });
    assert_eq!(
        alive_pid, first_pid,
        "PID walsender сменился — соединение было разорвано, а не поддержано keepalive"
    );

    // После паузы поток продолжает работать без рестарта воркера.
    emit_order(&source, 1, "после-паузы").await;
    wait_journal_count(&audit, 1).await;
    assert!(
        !handle.is_finished(),
        "воркер не должен завершаться на паузе"
    );

    shutdown_cleanly(token, handle).await;
    drop_db(&audit_admin, &audit_name).await;
    drop_db(&source_admin, &source_name).await;
}

/// AC #4: SIGTERM (CancellationToken) останавливает воркер чисто; после
/// рестарта журнал полон и без дублей.
#[tokio::test]
async fn sigterm_restart_keeps_journal_complete() {
    init_logging();
    let (audit_admin, source_admin, audit_url, source_url, audit_name, source_name) =
        spawn_env("sigterm").await;
    let source = connect_db(&source_url).await;
    let audit = connect_db(&audit_url).await;
    let slot_name = format!("waltrail_sigterm_{}", short_id());

    let config = worker_config(audit_url, source_url, slot_name.clone(), 1_000, 30_000);
    let token = tokio_util::sync::CancellationToken::new();
    let handle = tokio::spawn(worker_cdc::run(config.clone(), token.clone()));
    wait_worker_ready(&audit, &source, &source_admin, &slot_name).await;

    // Первая половина потока до сигнала.
    for id in 1..=3 {
        emit_order(&source, id, "до-sigterm").await;
    }
    wait_journal_count(&audit, 3).await;

    // Чистая остановка по сигналу с транзакцией «в полёте» (INSERT +
    // MESSAGE): незакоммиченный буфер должен быть переигран слотом после
    // рестарта — ровно один раз, без дублей и с контекстом.
    let mut tx = source.begin().await.expect("транзакция в полёте");
    sqlx::query("INSERT INTO orders VALUES (4, 1.00, 'в полёте', now())")
        .execute(&mut *tx)
        .await
        .expect("insert orders 4");
    sqlx::query(
        "SELECT pg_logical_emit_message(true, '_audit', \
         '{\"trace_id\": \"op-sigterm\"}')",
    )
    .execute(&mut *tx)
    .await
    .expect("emit_message _audit в полёте");
    tx.commit().await.expect("коммит транзакции в полёте");
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_cleanly(token, handle).await;

    // События, пришедшие, пока воркер остановлен.
    for id in 5..=6 {
        emit_order(&source, id, "после-sigterm").await;
    }

    // Рестарт: журнал доигрывается до полного, без дублей.
    let token2 = tokio_util::sync::CancellationToken::new();
    let handle2 = tokio::spawn(worker_cdc::run(config, token2.clone()));
    wait_journal_count(&audit, 6).await;

    assert_eq!(
        journal_duplicate_pairs(&audit).await,
        0,
        "после рестарта по SIGTERM дубликатов быть не должно"
    );
    let distinct_ids: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT after->>'id') FROM changes WHERE \"table\" = 'orders'",
    )
    .fetch_one(&audit)
    .await
    .expect("подсчёт уникальных строк orders");
    assert_eq!(
        distinct_ids, 6,
        "журнал полон: каждая строка источника на месте"
    );

    // Replay транзакции с MESSAGE: ровно одна строка события, payload и
    // trace_id на месте (AC тикета #8).
    let message_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM changes WHERE operation = 'MESSAGE'")
            .fetch_one(&audit)
            .await
            .expect("подсчёт MESSAGE-строк после рестарта");
    assert_eq!(
        message_rows, 1,
        "транзакция с MESSAGE должна быть переиграна ровно один раз"
    );
    let (trace_id, context): (Option<String>, serde_json::Value) =
        sqlx::query_as("SELECT trace_id, context FROM changes WHERE operation = 'MESSAGE'")
            .fetch_one(&audit)
            .await
            .expect("строка MESSAGE после рестарта");
    assert_eq!(trace_id.as_deref(), Some("op-sigterm"));
    assert_eq!(context, serde_json::json!({"trace_id": "op-sigterm"}));
    let order4_context: serde_json::Value = sqlx::query_scalar(
        "SELECT context FROM changes WHERE \"table\" = 'orders' AND after->>'id' = '4'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка orders 4 после рестарта");
    assert_eq!(
        order4_context,
        serde_json::json!({"trace_id": "op-sigterm"}),
        "строка изменения получила контекст MESSAGE своей транзакции"
    );

    shutdown_cleanly(token2, handle2).await;
    drop_db(&audit_admin, &audit_name).await;
    drop_db(&source_admin, &source_name).await;
}
