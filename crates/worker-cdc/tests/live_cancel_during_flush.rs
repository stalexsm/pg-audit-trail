//! Интеграционный тест тикета #46: отмена во время вставки завершает задачу
//! источника в пределах заданного бюджета (отзывчивая остановка).
//!
//! Вставка батча зажимается монопольной блокировкой таблицы `changes` с
//! отдельного соединения: токен отменяется, пока INSERT воркера ждёт
//! блокировку, — воркер обязан завершиться не позже бюджета, не растягивая
//! SIGTERM до таймаутов БД. Требует поднятой инфраструктуры: два PostgreSQL
//! из `docker compose up -d`.

mod common;

use std::sync::Once;
use std::time::{Duration, Instant};

use common::{
    application_name, audit_admin_url, cleanup_leftovers, connect_db, drop_db,
    register_source_with, setup_source, source_admin_url, source_name, source_slot, spawn_db,
    wait_sources_table, wait_until,
};
use sqlx::{Connection, PgConnection, PgPool};
use tokio_util::sync::CancellationToken;
use worker_cdc::WorkerConfig;

/// Бюджет завершения задачи после отмены во время вставке: с запасом к
/// таймаутам остановки (config.rs) — на незагруженном стенде выход занимает
/// доли секунды, зажатая блокировкой вставка прерывается немедленно.
const CANCEL_BUDGET: Duration = Duration::from_secs(10);

/// Логи воркера внутри теста: без подписчика warn-и остановки не видны.
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

/// Ждёт, пока хоть один бэкенд ждёт блокировку на таблице `changes`:
/// вставка воркера встала в очередь за ACCESS EXCLUSIVE блокировкой теста.
async fn wait_insert_blocked(audit: &PgPool) {
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_locks l \
             JOIN pg_class c ON c.oid = l.relation \
             WHERE c.relname = 'changes' AND NOT l.granted",
        )
        .fetch_one(audit)
        .await
        .unwrap_or(0)
            > 0
    })
    .await;
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

/// Вставляет строку в orders на источнике (одно событие CREATE в журнале).
async fn emit_order(source: &PgPool, id: i64, note: &str) {
    sqlx::query("INSERT INTO orders VALUES ($1, 1.00, $2, now())")
        .bind(id)
        .bind(note)
        .execute(source)
        .await
        .expect("insert orders");
}

/// AC #46: отмена во время вставки — задача завершается в пределах бюджета.
#[tokio::test]
async fn cancel_during_blocked_insert_finishes_within_budget() {
    init_logging();

    // Стенд: уникальные БД аудита и источника под префиксом сценария;
    // чистка остатков прошлых прогонов — по тому же префиксу.
    let audit_admin = connect_admin(&audit_admin_url()).await;
    let source_admin = connect_admin(&source_admin_url()).await;
    cleanup_leftovers(&audit_admin, "waltrail_audit_cancelflush_%").await;
    cleanup_leftovers(&source_admin, "waltrail_src_cancelflush_%").await;

    let (audit_url, audit_name) = spawn_db(
        &audit_admin,
        &audit_admin_url(),
        "waltrail_audit_cancelflush",
    )
    .await;
    let (source_url, source_db) = spawn_db(
        &source_admin,
        &source_admin_url(),
        "waltrail_src_cancelflush",
    )
    .await;

    let source = connect_db(&source_url).await;
    setup_source(&source).await;
    let audit = connect_db(&audit_url).await;

    let config = WorkerConfig {
        database_url: audit_url.clone(),
        reconnect_initial_backoff_ms: 1_000,
        reconnect_max_backoff_ms: 30_000,
    };
    let token = CancellationToken::new();
    let handle = tokio::spawn(worker_cdc::run(config, token.clone()));

    wait_sources_table(&audit).await;
    let source_key = source_name("cancelflush");
    let slot_name = source_slot(&source_key);
    register_source_with(
        &audit,
        &source_key,
        &source_url,
        &["public.schema_migrations"],
        "_audit",
        10,
    )
    .await;

    // Ждём подключения walsender (бутстрап воркера создал слот и стартовал).
    let app_name = application_name(&slot_name);
    wait_until(|| async {
        sqlx::query_scalar::<_, Option<i32>>(
            "SELECT pid FROM pg_stat_replication WHERE application_name = $1",
        )
        .bind(&app_name)
        .fetch_one(&source_admin)
        .await
        .ok()
        .flatten()
        .is_some()
    })
    .await;

    // Пайплайн жив: событие до блокировки доходит до журнала штатно.
    emit_order(&source, 1, "до блокировки").await;
    wait_journal_count(&audit, 1).await;

    // Зажимаем вставку: ACCESS EXCLUSIVE на changes с отдельного соединения.
    let mut lock_conn = PgConnection::connect(&audit_url)
        .await
        .expect("отдельное соединение для блокировки");
    let mut lock_tx = lock_conn
        .begin()
        .await
        .expect("транзакция блокировки changes");
    sqlx::query("LOCK TABLE changes IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock_tx)
        .await
        .expect("блокировка changes");

    // Событие в полёте: коммит источника придёт воркеру, вставка встанет
    // в очередь за блокировкой.
    emit_order(&source, 2, "под блокировкой").await;
    wait_insert_blocked(&audit).await;

    // Отмена во время вставки: задача обязана завершиться в пределах бюджета
    // — ретрай-цикл вставки прерывается токеном досрочно (тикет #46).
    let started = Instant::now();
    token.cancel();
    let result = tokio::time::timeout(CANCEL_BUDGET, handle)
        .await
        .expect("воркер не уложился в бюджет остановки после отмены во время вставки")
        .expect("таска воркера не запаниковала");
    assert!(
        result.is_ok(),
        "воркер должен завершиться штатно: {result:?}"
    );
    assert!(
        started.elapsed() <= CANCEL_BUDGET,
        "остановка превысила бюджет: {:?}",
        started.elapsed()
    );

    // Блокировку отпускаем; недосохранённое событие доиграет replay слота
    // при следующем старте — здесь воркер уже остановлен.
    lock_tx.rollback().await.expect("снятие блокировки");

    drop_db(&audit_admin, &audit_name).await;
    drop_db(&source_admin, &source_db).await;
}
