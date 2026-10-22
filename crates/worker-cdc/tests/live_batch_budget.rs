//! Интеграционный тест шва «бюджет параметров батчевой вставки» (тикет #42).
//!
//! Требует поднятой инфраструктуры (два PostgreSQL). Источник с верхней
//! границей `batch_size` (4095 — бюджет параметров протокола: 65535 на
//! statement, 16 bind-параметров на строку журнала) проходит вставку батча
//! без ошибки протокола: транзакция больше одного statement нарезается
//! на чанки в пределах бюджета. Значения выше бюджета, оставшиеся в реестре
//! от прежней верхней границы API (10 000, правка в обход API), тоже
//! инжестятся — до фикса они зацикливали реконнект и останавливали инжест
//! источника, копя WAL без ack.

mod common;

use std::time::Duration;

use common::{
    audit_admin_url, cleanup_leftovers, connect_db, drop_db, register_source_with, setup_source,
    source_admin_url, source_name, source_publication, source_slot, spawn_db, wait_publication,
    wait_slot, wait_sources_table, wait_until,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use worker_cdc::WorkerConfig;

/// Верхняя граница `batch_size` по контракту API — бюджет параметров
/// statement (65535 / 16 параметров на строку журнала); повторяет
/// `MAX_BATCH_SIZE` крейта api и `MAX_ROWS_PER_STATEMENT` крейта
/// worker-cdc. Регистрация здесь идёт прямым SQL, мимо валидации API:
/// тест ловит отказ воркера на полном чанке бюджета, а не дрейф
/// константы в api — тот закреплён юнит-тестом dto.rs.
const MAX_BATCH_SIZE: i32 = 4_095;

/// Таймаут штатной остановки воркера при teardown (как в live_two_postgres).
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Подключение к администраторскому пулу стенда (db1 / db-src).
async fn connect_admin(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите два PostgreSQL")
}

/// Сценарий бюджета: поднятые БД, пулы и таска воркера.
struct Scenario {
    audit_admin: PgPool,
    source_admin: PgPool,
    audit: PgPool,
    source: PgPool,
    source_key: String,
    audit_db: String,
    source_db: String,
    worker: tokio::task::JoinHandle<anyhow::Result<()>>,
    token: CancellationToken,
}

impl Scenario {
    /// Останавливает воркер штатно (закрытие walsender-соединения снимает
    /// активность слота) и сносит тестовые БД.
    async fn teardown(self) {
        self.token.cancel();
        let result = tokio::time::timeout(WORKER_SHUTDOWN_TIMEOUT, self.worker)
            .await
            .expect("воркер не завершился за 30 секунд после отмены")
            .expect("таска воркера не запаниковала");
        assert!(
            result.is_ok(),
            "воркер должен завершиться штатно: {result:?}"
        );

        drop_db(&self.audit_admin, &self.audit_db).await;
        drop_db(&self.source_admin, &self.source_db).await;
    }
}

/// Поднимает пару уникальных БД (аудит + источник) под префикс сценария,
/// запускает воркер и регистрирует источник с заданным `batch_size`.
/// Чистка остатков по тому же префиксу: параллельные сценарии изолированы.
async fn spawn_scenario(prefix: &str, batch_size: i32) -> Scenario {
    let audit_admin = connect_admin(&audit_admin_url()).await;
    let source_admin = connect_admin(&source_admin_url()).await;
    cleanup_leftovers(&audit_admin, &format!("waltrail_audit_{prefix}_%")).await;
    cleanup_leftovers(&source_admin, &format!("waltrail_src_{prefix}_%")).await;

    let (audit_url, audit_db) = spawn_db(
        &audit_admin,
        &audit_admin_url(),
        &format!("waltrail_audit_{prefix}"),
    )
    .await;
    let (source_url, source_db) = spawn_db(
        &source_admin,
        &source_admin_url(),
        &format!("waltrail_src_{prefix}"),
    )
    .await;

    let source_key = source_name(prefix);
    let source = connect_db(&source_url).await;
    setup_source(&source).await;

    let config = WorkerConfig {
        database_url: audit_url.clone(),
        reconnect_initial_backoff_ms: 1_000,
        reconnect_max_backoff_ms: 30_000,
    };
    let token = CancellationToken::new();
    let worker = tokio::spawn(worker_cdc::run(config, token.clone()));

    let audit = connect_db(&audit_url).await;
    wait_sources_table(&audit).await;
    register_source_with(
        &audit,
        &source_key,
        &source_url,
        &["public.schema_migrations"],
        "_audit",
        batch_size,
    )
    .await;

    wait_slot(&source, &source_slot(&source_key)).await;
    wait_publication(&source, &source_publication(&source_key)).await;

    Scenario {
        audit_admin,
        source_admin,
        audit,
        source,
        source_key,
        audit_db,
        source_db,
        worker,
        token,
    }
}

/// Эмитит одну транзакцию из `rows` вставок в `orders` и ждёт появления
/// всех строк в журнале. Ассерты: точный счёт журнала (вставка не упала
/// с ошибкой протокола — иначе счёт не сошёлся бы никогда), воркер
/// подключён без ошибок, слот подтверждён чекпоинтом.
async fn assert_tx_ingested(scenario: &Scenario, rows: i64) {
    sqlx::query(
        "INSERT INTO orders SELECT g, 1.00, 'бюджет', now() \
         FROM generate_series(1, $1::int8) AS g",
    )
    .bind(rows)
    .execute(&scenario.source)
    .await
    .expect("массовая вставка в orders");

    wait_until(|| async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM changes WHERE \"table\" = 'orders'")
            .fetch_one(&scenario.audit)
            .await
            .unwrap_or(0)
            == rows
    })
    .await;

    let (state, last_error): (String, Option<String>) =
        sqlx::query_as("SELECT worker_state, last_error FROM sources WHERE name = $1")
            .bind(&scenario.source_key)
            .fetch_one(&scenario.audit)
            .await
            .expect("строка источника в реестре");
    assert_eq!(state, "connected", "воркер источника подключён");
    assert!(last_error.is_none(), "ошибок инжеста нет: {last_error:?}");

    let checkpoint: Option<i64> =
        sqlx::query_scalar("SELECT last_commit_lsn FROM checkpoints WHERE name = $1")
            .bind(source_slot(&scenario.source_key))
            .fetch_optional(&scenario.audit)
            .await
            .expect("чтение чекпоинта");
    assert!(
        checkpoint.is_some_and(|lsn| lsn > 0),
        "слот подтверждён чекпоинтом"
    );
}

/// Источник с максимально допустимым `batch_size` (верхняя граница API)
/// проходит вставку батча без ошибки протокола: транзакция из двух полных
/// чанков исполняется двумя statement ровно по бюджету параметров
/// (4095 × 16 = 65520 ≤ 65535).
#[tokio::test]
async fn live_max_batch_size_ingests_within_parameter_budget() {
    // Уникальный префикс на сценарий (конвенция live-тестов): тесты одного
    // бинарника идут параллельно, общая чистка по «budget» снесла бы БД
    // соседа.
    let scenario = spawn_scenario("budgetmax", MAX_BATCH_SIZE).await;

    assert_tx_ingested(&scenario, 2 * i64::from(MAX_BATCH_SIZE)).await;

    scenario.teardown().await;
}

/// Значение `batch_size` выше бюджета (10 000 — историческая верхняя
/// граница API, может остаться в реестре после правки в обход API)
/// больше не роняет инжест: чанк сжимается до лимита statement.
#[tokio::test]
async fn live_over_budget_registry_value_still_ingests() {
    let scenario = spawn_scenario("budgetover", 10_000).await;

    assert_tx_ingested(&scenario, 10_000).await;

    scenario.teardown().await;
}
