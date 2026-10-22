//! Интеграционный тест потолка буфера транзакции (тикет #48).
//!
//! Требует поднятой инфраструктуры (TEST_DATABASE_URL / TEST_SOURCE_DATABASE_URL,
//! дефолт — `docker compose up -d`). Воркер запускается с уменьшенным через
//! настройку `CDC_TX_BUFFER_MAX_ROWS` потолком буфера; гигантская транзакция
//! клиента сверх потолка отвергается с явной ошибкой (статус `error` с текстом
//! в реестре), воркер жив и пишет другие источники, а не падает по OOM.

mod common;

use std::time::Duration;

use common::{
    audit_admin_url, cleanup_leftovers, connect_db, drop_db, register_source, setup_source,
    source_admin_url, source_name, source_publication, source_slot, spawn_db, wait_publication,
    wait_slot, wait_source_state, wait_sources_table, wait_until,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use worker_cdc::WorkerConfig;

/// Потолок буфера в тесте: гигантская транзакция (50 строк) заведомо выше,
/// обычные (2 строки) — заведомо ниже.
const TEST_TX_BUFFER_MAX_ROWS: usize = 5;

/// Размер гигантской транзакции клиента: одна транзакция, 50 изменений строк.
const GIANT_TX_ROWS: i64 = 50;

/// Подключение к администраторскому пулу стенда.
async fn connect_admin_pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите инфраструктуру тестов")
}

/// Запущенный воркер-супервизор над уникальной БД аудита.
struct Worker {
    token: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    audit: PgPool,
    audit_admin: PgPool,
    audit_name: String,
}

/// Поднимает воркер с потолком буфера, уменьшенным до TEST_TX_BUFFER_MAX_ROWS.
async fn spawn_worker(prefix: &str) -> Worker {
    let audit_admin = connect_admin_pool(&audit_admin_url()).await;
    cleanup_leftovers(&audit_admin, &format!("waltrail_audit_{prefix}_%")).await;
    let (audit_url, audit_name) = spawn_db(
        &audit_admin,
        &audit_admin_url(),
        &format!("waltrail_audit_{prefix}"),
    )
    .await;

    let config = WorkerConfig {
        database_url: audit_url.clone(),
        reconnect_initial_backoff_ms: 100,
        reconnect_max_backoff_ms: 500,
        tx_buffer_max_rows: TEST_TX_BUFFER_MAX_ROWS,
    };
    let token = tokio_util::sync::CancellationToken::new();
    let handle = tokio::spawn(worker_cdc::run(config, token.clone()));

    let audit = connect_db(&audit_url).await;
    wait_sources_table(&audit).await;

    Worker {
        token,
        handle,
        audit,
        audit_admin,
        audit_name,
    }
}

/// Штатное завершение воркера: процесс жив, без паники и ошибки.
async fn shutdown_worker(worker: Worker) {
    worker.token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(30), worker.handle)
        .await
        .expect("воркер не завершился за 30 секунд после отмены")
        .expect("таска воркера не запаниковала");
    assert!(
        result.is_ok(),
        "воркер должен завершиться штатно: {result:?}"
    );
    drop_db(&worker.audit_admin, &worker.audit_name).await;
}

/// Готовит уникальную БД клиента с тестовыми таблицами. Возвращает
/// (пул клиента, админ-пул стенда, URL, имя БД).
async fn spawn_client(prefix: &str) -> (PgPool, PgPool, String, String) {
    let admin = connect_admin_pool(&source_admin_url()).await;
    cleanup_leftovers(&admin, &format!("waltrail_src_{prefix}_%")).await;
    let (url, db) = spawn_db(
        &admin,
        &source_admin_url(),
        &format!("waltrail_src_{prefix}"),
    )
    .await;
    let pool = connect_db(&url).await;
    setup_source(&pool).await;
    (pool, admin, url, db)
}

/// Строки журнала источника в таблице `orders`.
async fn journal_orders(audit: &PgPool, source: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM changes WHERE source = $1 AND \"table\" = 'orders'")
        .bind(source)
        .fetch_one(audit)
        .await
        .unwrap_or(0)
}

/// AC (тикет #48): транзакция клиента сверх потолка буфера не роняет воркер —
/// попытка завершается ошибкой с диагностикой (статус `error`, текст с
/// потолком в реестре), гигантская транзакция не попадает в журнал, другой
/// источник продолжает писать, воркер завершается штатно по токену.
#[tokio::test]
async fn giant_transaction_fails_predictably_and_process_stays_alive() {
    let worker = spawn_worker("ceil").await;
    let (giant, giant_admin, giant_url, giant_db) = spawn_client("ceilg").await;
    let (other, other_admin, other_url, other_db) = spawn_client("ceilo").await;

    let giant_name = source_name("ceilg");
    let other_name = source_name("ceilo");
    register_source(&worker.audit, &giant_name, &giant_url).await;
    register_source(&worker.audit, &other_name, &other_url).await;

    wait_slot(&giant, &source_slot(&giant_name)).await;
    wait_publication(&giant, &source_publication(&giant_name)).await;
    wait_source_state(&worker.audit, &giant_name, "connected").await;
    wait_source_state(&worker.audit, &other_name, "connected").await;

    // Базовая линия: обычная транзакция (2 строки — ниже потолка) пишется
    // в штатном порядке.
    let mut baseline = giant.begin().await.expect("транзакция базовой линии");
    for id in 1..=2 {
        sqlx::query("INSERT INTO orders VALUES ($1, 1.00, 'база', now())")
            .bind(id)
            .execute(&mut *baseline)
            .await
            .expect("insert базовой линии");
    }
    baseline.commit().await.expect("коммит базовой линии");
    wait_until(|| async { journal_orders(&worker.audit, &giant_name).await == 2 }).await;

    // Гигантская транзакция: 50 изменений строк в ОДНОЙ транзакции —
    // сверх потолка буфера (5 строк).
    let mut giant_tx = giant.begin().await.expect("гигантская транзакция");
    for id in 10..(10 + GIANT_TX_ROWS) {
        sqlx::query("INSERT INTO orders VALUES ($1, 2.00, 'гигант', now())")
            .bind(id)
            .execute(&mut *giant_tx)
            .await
            .expect("insert гигантской транзакции");
    }
    giant_tx
        .commit()
        .await
        .expect("коммит гигантской транзакции");

    // Предсказуемая ошибка с диагностикой: статус `error`, текст называет
    // потолок буфера и xid гигантской транзакции.
    wait_source_state(&worker.audit, &giant_name, "error").await;
    let (state, last_error): (String, Option<String>) =
        sqlx::query_as("SELECT worker_state, last_error FROM sources WHERE name = $1")
            .bind(&giant_name)
            .fetch_one(&worker.audit)
            .await
            .expect("статус источника с гигантской транзакцией");
    assert_eq!(state, "error");
    let error_text = last_error.expect("текст ошибки виден в реестре");
    assert!(
        error_text.contains("потолок буфера транзакции"),
        "диагностика называет потолок буфера: {error_text}"
    );
    assert!(
        error_text.contains("потолке 5"),
        "диагностика называет настроенный потолок ({}): {error_text}",
        TEST_TX_BUFFER_MAX_ROWS
    );

    // Гигантская транзакция в журнал не попала: только 2 строки базовой линии.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        journal_orders(&worker.audit, &giant_name).await,
        2,
        "строки гигантской транзакции не пишутся, базовая линия цела"
    );

    // Процесс жив и пишет другие источники: исправный источник после
    // ошибки гигантской транзакции продолжает попадать в журнал.
    sqlx::query("INSERT INTO orders VALUES (1, 1.00, 'после гиганта', now())")
        .execute(&other)
        .await
        .expect("insert исправного источника после ошибки");
    wait_until(|| async { journal_orders(&worker.audit, &other_name).await >= 1 }).await;

    // Задача воркера не завершилась (не OOM, не паника) и завершается штатно.
    assert!(
        !worker.handle.is_finished(),
        "воркер жив после гигантской транзакции"
    );

    shutdown_worker(worker).await;
    drop_db(&giant_admin, &giant_db).await;
    drop_db(&other_admin, &other_db).await;
}
