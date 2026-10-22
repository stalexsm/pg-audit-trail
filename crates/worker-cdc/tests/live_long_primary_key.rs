//! Интеграционный тест тикета #44: ключ строки длиннее ширины колонки
//! `primary_key` VARCHAR(255) не останавливает инжест источника. Итоговое
//! закодированное значение ключа (включая составной ключ, закодированный
//! JSON-массивом) обрезается по границе символа, следующие изменения
//! продолжают попадать в журнал.
//!
//! Требует поднятой инфраструктуры: `TEST_DATABASE_URL` и
//! `TEST_SOURCE_DATABASE_URL` (два PostgreSQL стенда).

mod common;

use std::time::Duration;

use common::{
    audit_admin_url, cleanup_leftovers, connect_db, drop_db, register_source, source_admin_url,
    source_name, source_publication, source_slot, spawn_db, wait_publication, wait_slot,
    wait_source_state, wait_sources_table, wait_until,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use worker_cdc::WorkerConfig;

/// Ширина колонки `primary_key` таблицы `changes` (VARCHAR(255)).
const PRIMARY_KEY_COLUMN_WIDTH: usize = 255;

/// Подключение к администраторскому пулу стенда.
async fn connect_admin(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите два PostgreSQL")
}

#[tokio::test]
async fn live_long_primary_key_is_truncated_and_ingest_continues() {
    let audit_admin = connect_admin(&audit_admin_url()).await;
    let source_admin = connect_admin(&source_admin_url()).await;

    // Остатки упавших прогонов мешают новому; префиксы этого сценария
    // изолируют его от параллельных тестов.
    cleanup_leftovers(&audit_admin, "waltrail_lpk_%").await;
    cleanup_leftovers(&source_admin, "waltrail_src_lpk_%").await;

    let (audit_url, audit_name) = spawn_db(&audit_admin, &audit_admin_url(), "waltrail_lpk").await;
    let (source_url, source_db) =
        spawn_db(&source_admin, &source_admin_url(), "waltrail_src_lpk").await;

    let source_key = source_name("lpk");
    let slot_name = source_slot(&source_key);

    let source = connect_db(&source_url).await;
    // Таблица с текстовым естественным ключом и таблица с составным
    // текстовым ключом: закодированный JSON-массив превысит границу
    // колонки, хотя каждая компонента влезает.
    sqlx::query("CREATE TABLE wide_keys (key TEXT PRIMARY KEY, payload TEXT)")
        .execute(&source)
        .await
        .expect("создание wide_keys");
    sqlx::query("CREATE TABLE composite_wide (a TEXT, b TEXT, payload TEXT, PRIMARY KEY (a, b))")
        .execute(&source)
        .await
        .expect("создание composite_wide");

    let config = WorkerConfig {
        database_url: audit_url.clone(),
        reconnect_initial_backoff_ms: 1_000,
        reconnect_max_backoff_ms: 30_000,
        tx_buffer_max_rows: 100_000,
    };

    let token = tokio_util::sync::CancellationToken::new();
    let worker_token = token.clone();
    let handle = tokio::spawn(worker_cdc::run(config, worker_token));

    let audit = connect_db(&audit_url).await;
    wait_sources_table(&audit).await;
    register_source(&audit, &source_key, &source_url).await;

    wait_slot(&source, &slot_name).await;
    wait_publication(&source, &source_publication(&source_key)).await;

    // Транзакция 1: мультибайтовый текстовый ключ 540 байт (300 символов).
    // «ключ-» — 9 байт: после 28 полных повторов (байты 0–251) символ «к»
    // занимает байты 252–253, байт 254 — начало «л» и валидная граница:
    // срез отступает от предела 255 к 254 байтам, символы не рвутся.
    let long_multibyte_key = "ключ-".repeat(60);
    sqlx::query("INSERT INTO wide_keys VALUES ($1, 'первый')")
        .bind(&long_multibyte_key)
        .execute(&source)
        .await
        .expect("insert wide_keys с длинным ключом");

    wait_until(|| async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM changes WHERE \"table\" = 'wide_keys'")
            .fetch_one(&audit)
            .await
            .unwrap_or(0)
            == 1
    })
    .await;

    let first =
        sqlx::query("SELECT primary_key, operation FROM changes WHERE \"table\" = 'wide_keys'")
            .fetch_one(&audit)
            .await
            .expect("строка с длинным ключом в журнале");
    let expected_truncated = format!("{}к", "ключ-".repeat(28));
    assert_eq!(
        first.get::<Option<String>, _>("primary_key").as_deref(),
        Some(expected_truncated.as_str()),
        "ключ обрезан по границе символа, а не остановил инжест"
    );
    assert_eq!(first.get::<String, _>("operation"), "CREATE");

    // Транзакция 2: следующее изменение с длинным ASCII-ключом (300 байт)
    // доезжает до журнала — «отравленная» транзакция не зациклила
    // реконнект и не скопила неподтверждённый WAL слота.
    sqlx::query("INSERT INTO wide_keys VALUES ($1, 'второй')")
        .bind("k".repeat(300))
        .execute(&source)
        .await
        .expect("insert wide_keys после длинного ключа");

    wait_until(|| async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM changes WHERE \"table\" = 'wide_keys'")
            .fetch_one(&audit)
            .await
            .unwrap_or(0)
            == 2
    })
    .await;
    let expected_ascii_truncated = "k".repeat(PRIMARY_KEY_COLUMN_WIDTH);
    let second_key: Option<String> = sqlx::query_scalar(
        "SELECT primary_key FROM changes \
         WHERE \"table\" = 'wide_keys' AND after->>'payload' = 'второй'",
    )
    .fetch_one(&audit)
    .await
    .expect("строка после длинного ключа в журнале");
    assert_eq!(
        second_key.as_deref(),
        Some(expected_ascii_truncated.as_str())
    );

    // Транзакция 3: составной текстовый ключ — компоненты по 200 символов,
    // закодированный JSON-массив (407 байт) длиннее колонки. Обрезается
    // итоговое закодированное значение: `["aaa…","bbb…` на байте 255.
    sqlx::query("INSERT INTO composite_wide VALUES ($1, $2, 'составной')")
        .bind("a".repeat(200))
        .bind("b".repeat(200))
        .execute(&source)
        .await
        .expect("insert composite_wide");

    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM changes WHERE \"table\" = 'composite_wide'",
        )
        .fetch_one(&audit)
        .await
        .unwrap_or(0)
            == 1
    })
    .await;
    let composite_key: Option<String> =
        sqlx::query_scalar("SELECT primary_key FROM changes WHERE \"table\" = 'composite_wide'")
            .fetch_one(&audit)
            .await
            .expect("строка с составным ключом в журнале");
    // Контракт тот же, что для trace_id: срез по границе символа —
    // закрывающая кавычка и скобка JSON при этом не сохраняются.
    let a = "a".repeat(200);
    let b = "b".repeat(50);
    let expected_composite_truncated = format!("[\"{a}\",\"{b}");
    assert_eq!(
        composite_key.as_deref(),
        Some(expected_composite_truncated.as_str()),
        "обрезано итоговое закодированное значение массива без починки JSON"
    );

    // Инжест жив: задача источника подключена, ошибок нет, чекпоинт
    // продвинулся за пределы всех трёх транзакций.
    wait_source_state(&audit, &source_key, "connected").await;
    let last_error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM sources WHERE name = $1")
            .bind(&source_key)
            .fetch_one(&audit)
            .await
            .expect("статус источника в реестре");
    assert!(
        last_error.is_none(),
        "после подключения текст ошибки закрыт: {last_error:?}"
    );
    let checkpoint: Option<i64> =
        sqlx::query_scalar("SELECT last_commit_lsn FROM checkpoints WHERE name = $1")
            .bind(&slot_name)
            .fetch_one(&audit)
            .await
            .expect("чекпоинт источника");
    assert!(checkpoint.is_some(), "слот подтверждён после коммита");

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
    drop_db(&source_admin, &source_db).await;
}
