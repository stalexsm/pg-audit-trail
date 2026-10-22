//! Live-тест тикета #43: источник с паролем со спецсимволами (@, :, /, %)
//! подключается репликационным трактом (walsender) так же, как sqlx-трактом
//! (бутстрап). URL регистрируется percent-encoded — так его кодирует любой
//! URL-клиент; воркер обязан декодировать компоненты перед аутентификацией.
//!
//! Требует поднятой инфраструктуры: `TEST_DATABASE_URL` (db1) и
//! `TEST_SOURCE_DATABASE_URL` (db-src). Ассерты: источник выходит из
//! `error` в `connected`, публикация и слот созданы бутстрапом, инжест
//! изменения доходит до журнала `changes`.

mod common;

use std::sync::Once;
use std::time::Duration;

use anyhow::bail;
use common::{
    audit_admin_url, cleanup_leftovers, connect_db, drop_db, register_source, setup_source,
    source_admin_url, source_name, source_publication, source_slot, spawn_db, wait_publication,
    wait_slot, wait_source_state, wait_sources_table, wait_until,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use worker_cdc::WorkerConfig;

/// Пароль источника со всеми спецсимволами из тикета: @, :, /, %.
const SPECIAL_PASSWORD: &str = "p@ss:w/rd%";

/// Таймаут штатного завершения воркера после отмены.
const WORKER_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Логи воркера внутри теста: без подписчика warn-и и статусы задач не видны.
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

async fn connect_admin(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — проверьте TEST_*_DATABASE_URL")
}

#[tokio::test]
async fn live_source_with_special_password_replicates() {
    init_logging();

    let audit_admin = connect_admin(&audit_admin_url()).await;
    let source_admin = connect_admin(&source_admin_url()).await;

    // Остатки упавших прогонов (БД + их слоты) мешают новому прогону.
    cleanup_leftovers(&audit_admin, "waltrail_audit_%").await;
    cleanup_leftovers(&source_admin, "waltrail_src_%").await;

    let (audit_url, audit_name) =
        spawn_db(&audit_admin, &audit_admin_url(), "waltrail_audit").await;
    let (source_url_plain, source_db) =
        spawn_db(&source_admin, &source_admin_url(), "waltrail_src").await;
    let role = source_name("wtpct");

    // Воркер принадлежит тесту, а не сценарию: его остановка выполняется
    // при любом исходе сценария до уборки — активный walsender-слот живого
    // воркера не даёт сделать DROP DATABASE.
    let token = CancellationToken::new();
    let handle = tokio::spawn(worker_cdc::run(
        WorkerConfig {
            database_url: audit_url.clone(),
            reconnect_initial_backoff_ms: 1_000,
            reconnect_max_backoff_ms: 30_000,
            tx_buffer_max_rows: 100_000,
        },
        token.clone(),
    ));

    // Сценарий — в отдельной задаче: JoinHandle ловит и Err, и панику внутри
    // сценария (хелперы ожидания паникуют по таймауту), поэтому остановка
    // воркера и уборка выполняются при любом исходе — роль с паролем со
    // спецсимволами не остаётся на стенде.
    let scenario = tokio::spawn({
        let source_admin = source_admin.clone();
        let audit_url = audit_url.clone();
        let source_url_plain = source_url_plain.clone();
        let role = role.clone();
        async move { run_scenario(&source_admin, &audit_url, &source_url_plain, &role).await }
    })
    .await;

    token.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(WORKER_SHUTDOWN_TIMEOUT_SECS), handle)
        .await
        .expect("воркер не завершился за 30 секунд после отмены");
    cleanup_resources(&audit_admin, &source_admin, &audit_name, &source_db, &role).await;

    match scenario {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("сценарий живого теста не прошёл: {e:#}"),
        Err(join) => panic!("сценарий живого теста запаниковал: {join}"),
    }
    // Штатное завершение воркера — часть проверяемого поведения.
    assert!(
        stopped.is_ok(),
        "воркер должен завершиться штатно: {stopped:?}"
    );
}

/// Уборка стенда: БД (вместе со слотом и публикацией) и роль сносятся —
/// пароль со спецсимволами не остаётся в чужих ролях. Выполняется только
/// после остановки воркера.
async fn cleanup_resources(
    audit_admin: &PgPool,
    source_admin: &PgPool,
    audit_name: &str,
    source_db: &str,
    role: &str,
) {
    drop_db(audit_admin, audit_name).await;
    drop_db(source_admin, source_db).await;
    // IF EXISTS: при падении сценария до CREATE ROLE роль мог не появиться.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP ROLE IF EXISTS \"{role}\""
    )))
    .execute(source_admin)
    .await
    .expect("удаление роли источника");
}

/// Сценарий теста: регистрация источника с percent-encoded кредами,
/// бутстрап, подключение репликации, инжест. Запуск и остановка воркера —
/// в тестовой функции.
async fn run_scenario(
    source_admin: &PgPool,
    audit_url: &str,
    source_url_plain: &str,
    role: &str,
) -> anyhow::Result<()> {
    let slot_name = source_slot(role);
    let publication = source_publication(role);

    // Роль источника с паролем со спецсимволами. Суперпользователь — потому
    // что бутстрап воркера сам создаёт публикацию FOR ALL TABLES (право
    // суперпользователя) и логический слот; проверяем тракты подключения,
    // а не разделение прав. Имя и пароль — из фикстуры этого же теста.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE \"{role}\" LOGIN SUPERUSER PASSWORD '{SPECIAL_PASSWORD}'"
    )))
    .execute(source_admin)
    .await
    .map_err(|e| anyhow::Error::new(e).context("создание роли источника"))?;

    // URL регистрации — percent-encoded креды: url-крейт кодирует пароль
    // при set_password, регистрация получает ровно то, что дал бы клиент.
    let mut registered = url::Url::parse(source_url_plain).expect("разбор URL источника");
    registered.set_username(role).expect("имя роли");
    registered
        .set_password(Some(SPECIAL_PASSWORD))
        .expect("пароль со спецсимволами");

    let source = connect_db(source_url_plain).await;
    setup_source(&source).await;

    let audit = connect_db(audit_url).await;

    // Регистрация источника в реестре — после применения миграций воркером.
    wait_sources_table(&audit).await;
    register_source(&audit, role, registered.as_str()).await;

    // Бутстрап (sqlx-тракт) обязан отработать по percent-encoded URL.
    wait_slot(&source, &slot_name).await;
    wait_publication(&source, &publication).await;

    // Ассерт тикета: источник выходит из error в connected — репликационный
    // тракт (walsender) прошёл аутентификацию тем же паролем.
    wait_source_state(&audit, role, "connected").await;

    // Инжест идёт: изменение на источнике доходит до журнала.
    sqlx::query("INSERT INTO orders VALUES (1, 10.50, 'спецсимволы', now())")
        .execute(&source)
        .await
        .expect("insert orders");

    wait_until(|| async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM changes WHERE \"table\" = 'orders'")
            .fetch_one(&audit)
            .await
            .unwrap_or(0)
            == 1
    })
    .await;

    let note: String =
        sqlx::query_scalar("SELECT after->>'note' FROM changes WHERE \"table\" = 'orders'")
            .fetch_one(&audit)
            .await
            .expect("строка orders в журнале");
    if note != "спецсимволы" {
        bail!("образ строки искажён: {note}");
    }
    Ok(())
}
