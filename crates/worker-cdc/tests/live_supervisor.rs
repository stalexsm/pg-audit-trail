//! Интеграционный тест шва «супервизор источников» (ADR-0005, тикет #30).
//!
//! Требует поднятой инфраструктуры: `docker compose up -d`. Воркер-супервизор
//! запускается из библиотечной части поверх уникальной БД аудита; источники
//! регистрируются в реестре чистым SQL (как POST /api/sources). Проверяется
//! внешнее поведение через наблюдаемые эффекты: подхват без перезапуска,
//! параллельная запись двух источников с изоляцией упавшего клиента,
//! применение правок настроек живого источника, остановка задач помеченных
//! к удалению и исчезнувших из реестра источников.

mod common;

use std::time::Duration;

use common::{
    application_name, audit_admin_url, cleanup_leftovers, connect_db, drop_db,
    journal_duplicate_pairs, register_source, register_source_with, setup_source, source_admin_url,
    source_name, source_publication, source_slot, spawn_db, wait_publication, wait_slot,
    wait_source_state, wait_sources_table, wait_until,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use worker_cdc::WorkerConfig;

/// Подключение к администраторскому пулу стенда (db1 / db-src).
async fn connect_admin(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("нет подключения к стенду — поднимите `docker compose up -d`")
}

/// Запущенный воркер-супервизор над уникальной БД аудита.
struct Worker {
    token: tokio_util::sync::CancellationToken,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    audit: PgPool,
    audit_admin: PgPool,
    audit_name: String,
}

/// Поднимает воркер-супервизор над уникальной БД аудита и ждёт применения
/// миграций (таблица реестра). Реестр на старт пуст: источники регистрируются
/// тестами после старта — подхват без перезапуска есть предмет проверки.
async fn spawn_worker(prefix: &str) -> Worker {
    let audit_admin = connect_admin(&audit_admin_url()).await;
    cleanup_leftovers(&audit_admin, &format!("waltrail_audit_{prefix}_%")).await;
    let (audit_url, audit_name) = spawn_db(
        &audit_admin,
        &audit_admin_url(),
        &format!("waltrail_audit_{prefix}"),
    )
    .await;

    let config = WorkerConfig {
        database_url: audit_url.clone(),
        reconnect_initial_backoff_ms: 1_000,
        reconnect_max_backoff_ms: 30_000,
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

/// Штатное завершение воркера: без паники и ошибки, БД аудита снесена.
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
    let admin = connect_admin(&source_admin_url()).await;
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

/// PID активного walsender-бэкенда соединения задачи источника в
/// `pg_stat_replication` (нет живого соединения — `None`).
async fn walsender_pid(source_admin: &PgPool, app_name: &str) -> Option<i32> {
    sqlx::query_scalar("SELECT pid FROM pg_stat_replication WHERE application_name = $1")
        .bind(app_name)
        .fetch_optional(source_admin)
        .await
        .unwrap_or(None)
}

/// AC: зарегистрированный источник подхватывается без перезапуска — воркер
/// уже работает, источник появляется в реестре позже. Изменения клиента
/// попадают в журнал с его ключом; статус в реестре показывает подключение
/// и время последнего события.
#[tokio::test]
async fn registered_source_is_picked_up_without_restart() {
    let worker = spawn_worker("pickup").await;
    let (source, source_admin, source_url, source_db) = spawn_client("pickup").await;

    // Регистрация ПОСЛЕ старта воркера: подхват обязан произойти без
    // перезапуска — слот и публикация создаются бутстрапом задачи.
    let name = source_name("pickup");
    register_source(&worker.audit, &name, &source_url).await;

    wait_slot(&source, &source_slot(&name)).await;
    wait_publication(&source, &source_publication(&name)).await;
    wait_source_state(&worker.audit, &name, "connected").await;

    // Изменение клиента + контекст операции (ADR-0004) попадают в журнал
    // с ключом источника.
    sqlx::query("INSERT INTO orders VALUES (1, 1.00, 'подхвачен', now())")
        .execute(&source)
        .await
        .expect("insert orders 1");
    let mut tx = source.begin().await.expect("транзакция с MESSAGE");
    sqlx::query("INSERT INTO orders VALUES (2, 2.00, 'с контекстом', now())")
        .execute(&mut *tx)
        .await
        .expect("insert orders 2");
    sqlx::query("SELECT pg_logical_emit_message(true, '_audit', '{\"trace_id\": \"op-pickup\"}')")
        .execute(&mut *tx)
        .await
        .expect("emit_message _audit");
    tx.commit().await.expect("коммит с MESSAGE");

    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM changes WHERE source = $1 AND \"table\" = 'orders'",
        )
        .bind(&name)
        .fetch_one(&worker.audit)
        .await
        .unwrap_or(0)
            == 2
    })
    .await;

    // Ключ источника стоит на всех строках; контекст MESSAGE — на своей транзакции.
    let foreign: i64 = sqlx::query_scalar("SELECT count(*) FROM changes WHERE source <> $1")
        .bind(&name)
        .fetch_one(&worker.audit)
        .await
        .expect("подсчёт чужих строк");
    assert_eq!(foreign, 0, "все строки журнала помечены ключом источника");
    let (trace_id, context): (Option<String>, serde_json::Value) = sqlx::query_as(
        "SELECT trace_id, context FROM changes \
         WHERE source = $1 AND \"table\" = 'orders' AND after->>'id' = '2'",
    )
    .bind(&name)
    .fetch_one(&worker.audit)
    .await
    .expect("строка с MESSAGE в журнале");
    assert_eq!(trace_id.as_deref(), Some("op-pickup"));
    assert_eq!(context, serde_json::json!({"trace_id": "op-pickup"}));

    // Статус: время последнего события записано, текста ошибки нет.
    wait_until(|| async {
        sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
            "SELECT last_event_at FROM sources WHERE name = $1",
        )
        .bind(&name)
        .fetch_one(&worker.audit)
        .await
        .unwrap_or(None)
        .is_some()
    })
    .await;
    let (state, last_error): (String, Option<String>) =
        sqlx::query_as("SELECT worker_state, last_error FROM sources WHERE name = $1")
            .bind(&name)
            .fetch_one(&worker.audit)
            .await
            .expect("статус источника");
    assert_eq!(state, "connected");
    assert!(last_error.is_none(), "ошибок подключения не было");

    shutdown_worker(worker).await;
    drop_db(&source_admin, &source_db).await;
}

/// AC: два источника пишут параллельно, каждый со своим ключом и своим
/// чекпоинтом; упавший клиент (соединение отклоняется) не блокирует остальные
/// источники — его состояние видно в реестре как ошибка с текстом.
#[tokio::test]
async fn two_sources_ingest_in_parallel_and_broken_client_does_not_block() {
    let worker = spawn_worker("par").await;
    let (client_a, admin_a, url_a, db_a) = spawn_client("para").await;
    let (client_b, admin_b, url_b, db_b) = spawn_client("parb").await;

    let name_a = source_name("para");
    let name_b = source_name("parb");
    register_source(&worker.audit, &name_a, &url_a).await;
    register_source(&worker.audit, &name_b, &url_b).await;

    wait_source_state(&worker.audit, &name_a, "connected").await;
    wait_source_state(&worker.audit, &name_b, "connected").await;

    // Третий источник — упавший клиент: соединение отклоняется, задача
    // повторяет с backoff, состояние ошибки видно в реестре.
    let name_bad = source_name("parx");
    register_source(
        &worker.audit,
        &name_bad,
        "postgresql://postgres:postgres@127.0.0.1:1/postgres",
    )
    .await;
    wait_source_state(&worker.audit, &name_bad, "error").await;
    let (state, last_error): (String, Option<String>) =
        sqlx::query_as("SELECT worker_state, last_error FROM sources WHERE name = $1")
            .bind(&name_bad)
            .fetch_one(&worker.audit)
            .await
            .expect("статус упавшего источника");
    assert_eq!(state, "error");
    assert!(
        last_error.is_some_and(|text| !text.is_empty()),
        "текст ошибки виден в реестре"
    );

    // Оба исправных источника продолжают писать параллельно, каждый со своим ключом.
    sqlx::query("INSERT INTO orders VALUES (1, 1.00, 'источник A', now())")
        .execute(&client_a)
        .await
        .expect("insert A");
    sqlx::query("INSERT INTO orders VALUES (11, 11.00, 'источник B', now())")
        .execute(&client_b)
        .await
        .expect("insert B");

    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM (SELECT DISTINCT source FROM changes WHERE source IN ($1, $2)) s",
        )
        .bind(&name_a)
        .bind(&name_b)
        .fetch_one(&worker.audit)
        .await
        .unwrap_or(0)
            == 2
    })
    .await;

    // Строки не смешались: по ключу виден источник каждой.
    let note_a: String = sqlx::query_scalar(
        "SELECT after->>'note' FROM changes WHERE source = $1 AND \"table\" = 'orders'",
    )
    .bind(&name_a)
    .fetch_one(&worker.audit)
    .await
    .expect("строка источника A");
    assert_eq!(note_a, "источник A");
    let note_b: String = sqlx::query_scalar(
        "SELECT after->>'note' FROM changes WHERE source = $1 AND \"table\" = 'orders'",
    )
    .bind(&name_b)
    .fetch_one(&worker.audit)
    .await
    .expect("строка источника B");
    assert_eq!(note_b, "источник B");

    // Чекпоинты независимы: по одному на слот каждого источника, оба продвинуты.
    let checkpoints: Vec<String> =
        sqlx::query_scalar("SELECT name FROM checkpoints WHERE last_commit_lsn > 0 ORDER BY name")
            .fetch_all(&worker.audit)
            .await
            .expect("чекпоинты источников");
    assert_eq!(
        checkpoints,
        vec![source_slot(&name_a), source_slot(&name_b)],
        "по чекпоинту на каждый источник"
    );

    // Дедупликация по каждому источнику: повторов пар нет (ключ включает источник).
    let dupes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM (SELECT source, commit_lsn, event_index FROM changes \
         GROUP BY source, commit_lsn, event_index HAVING count(*) > 1) d",
    )
    .fetch_one(&worker.audit)
    .await
    .expect("подсчёт дублирующихся пар с ключом источника");
    assert_eq!(dupes, 0);
    assert_eq!(journal_duplicate_pairs(&worker.audit).await, 0);

    shutdown_worker(worker).await;
    drop_db(&admin_a, &db_a).await;
    drop_db(&admin_b, &db_b).await;
}

/// Правки настроек живого источника (игнор-таблицы) применяются без
/// пересоздания источника: супервизор перезапускает задачу с настройками
/// из реестра.
#[tokio::test]
async fn settings_edit_applies_to_live_source() {
    let worker = spawn_worker("edit").await;
    let (source, source_admin, source_url, source_db) = spawn_client("edit").await;

    let name = source_name("edit");
    // Дефолт API: игнор-таблицы типовых миграторов (в схеме дефолт — пустой
    // список, его дополняет POST /api/sources).
    register_source_with(
        &worker.audit,
        &name,
        &source_url,
        &[
            "public.alembic_version",
            "public.schema_migrations",
            "public._sqlx_migrations",
            "public.django_migrations",
        ],
        "_audit",
        500,
    )
    .await;
    wait_source_state(&worker.audit, &name, "connected").await;

    // Дефолтный игнор-лист: schema_migrations не пишется, orders пишется.
    sqlx::query("INSERT INTO schema_migrations VALUES (1, 'до-правки')")
        .execute(&source)
        .await
        .expect("insert schema_migrations");
    sqlx::query("INSERT INTO orders VALUES (1, 1.00, 'до-правки', now())")
        .execute(&source)
        .await
        .expect("insert orders 1");
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM changes WHERE source = $1 AND \"table\" = 'orders'",
        )
        .bind(&name)
        .fetch_one(&worker.audit)
        .await
        .unwrap_or(0)
            == 1
    })
    .await;
    // К моменту появления orders вставка в schema_migrations уже обработана.
    let ignored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM changes WHERE source = $1 AND \"table\" = 'schema_migrations'",
    )
    .bind(&name)
    .fetch_one(&worker.audit)
    .await
    .expect("подсчёт игнорируемых строк");
    assert_eq!(ignored, 0, "служебная таблица в журнал не попадает");

    // Правка реестра: orders уходит в игнор-лист. Супервизор перезапускает
    // задачу в пределах интервала опроса (константа супервизора — 5 с)
    // плюс запас на остановку и бутстрап.
    sqlx::query(
        "UPDATE sources SET ignore_tables = \
         ARRAY['public.alembic_version', 'public.schema_migrations', \
         'public._sqlx_migrations', 'public.django_migrations', 'public.orders'] \
         WHERE name = $1",
    )
    .bind(&name)
    .execute(&worker.audit)
    .await
    .expect("правка игнор-листа источника");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Новая задача жива (events пишется) и orders больше не собирает.
    sqlx::query("INSERT INTO events VALUES (1, 'после-правки')")
        .execute(&source)
        .await
        .expect("insert events после правки");
    sqlx::query("INSERT INTO orders VALUES (2, 2.00, 'после-правки', now())")
        .execute(&source)
        .await
        .expect("insert orders 2 после правки");
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM changes WHERE source = $1 AND \"table\" = 'events'",
        )
        .bind(&name)
        .fetch_one(&worker.audit)
        .await
        .unwrap_or(0)
            == 1
    })
    .await;
    // events дошёл — вставка orders (раньше по WAL) уже обработана и проигнорирована.
    let orders: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM changes WHERE source = $1 AND \"table\" = 'orders'",
    )
    .bind(&name)
    .fetch_one(&worker.audit)
    .await
    .expect("подсчёт orders после правки");
    assert_eq!(orders, 1, "orders ушёл в игнор-лист после правки реестра");

    shutdown_worker(worker).await;
    drop_db(&source_admin, &source_db).await;
}

/// Пометка к удалению (DELETE /api/sources → 202) останавливает задачу:
/// walsender-соединение исчезает, новые изменения не собираются. Строка
/// реестра с меткой ждёт исполнения удаления (тикет #31).
#[tokio::test]
async fn delete_mark_stops_task_and_keeps_row_for_execution() {
    let worker = spawn_worker("delete").await;
    let (source, source_admin, source_url, source_db) = spawn_client("delete").await;

    let name = source_name("delete");
    register_source(&worker.audit, &name, &source_url).await;
    let app_name = application_name(&source_slot(&name));
    wait_source_state(&worker.audit, &name, "connected").await;

    sqlx::query("INSERT INTO orders VALUES (1, 1.00, 'до-удаления', now())")
        .execute(&source)
        .await
        .expect("insert orders до удаления");
    wait_until(|| async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM changes WHERE source = $1")
            .bind(&name)
            .fetch_one(&worker.audit)
            .await
            .unwrap_or(0)
            == 1
    })
    .await;

    // Пометка к удалению — как DELETE /api/sources.
    sqlx::query("UPDATE sources SET deleted_at = now() WHERE name = $1")
        .bind(&name)
        .execute(&worker.audit)
        .await
        .expect("пометка источника к удалению");

    // Задача остановлена: walsender-соединение исчезло.
    wait_until(|| async { walsender_pid(&source_admin, &app_name).await.is_none() }).await;

    // Сбор прекращён: новые изменения не попадают в журнал.
    sqlx::query("INSERT INTO orders VALUES (2, 2.00, 'после-удаления', now())")
        .execute(&source)
        .await
        .expect("insert orders после удаления");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM changes WHERE source = $1")
        .bind(&name)
        .fetch_one(&worker.audit)
        .await
        .expect("подсчёт журнала после удаления");
    assert_eq!(total, 1, "после пометки к удалению сбор прекращён");

    // Строка реестра с меткой ждёт исполнения удаления (тикет #31):
    // дроп слота и публикации у клиента и снятие строки — за воркером.
    let deleted_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM sources WHERE name = $1")
            .bind(&name)
            .fetch_one(&worker.audit)
            .await
            .expect("строка удалённого источника");
    assert!(deleted_at.is_some(), "метка удаления на месте");

    shutdown_worker(worker).await;
    drop_db(&source_admin, &source_db).await;
}

/// Источник, исчезнувший из реестра (строку сняли в обход DELETE),
/// останавливается: воркер не держит задачу без записи в реестре.
#[tokio::test]
async fn vanished_source_stops_task() {
    let worker = spawn_worker("vanish").await;
    let (_source, source_admin, source_url, source_db) = spawn_client("vanish").await;

    let name = source_name("vanish");
    register_source(&worker.audit, &name, &source_url).await;
    let app_name = application_name(&source_slot(&name));
    wait_source_state(&worker.audit, &name, "connected").await;
    assert!(
        walsender_pid(&source_admin, &app_name).await.is_some(),
        "задача источника держит walsender-соединение"
    );

    // Строка реестра снята в обход DELETE.
    sqlx::query("DELETE FROM sources WHERE name = $1")
        .bind(&name)
        .execute(&worker.audit)
        .await
        .expect("снятие строки реестра");

    wait_until(|| async { walsender_pid(&source_admin, &app_name).await.is_none() }).await;
    assert!(
        walsender_pid(&source_admin, &app_name).await.is_none(),
        "задача исчезнувшего источника остановлена"
    );

    shutdown_worker(worker).await;
    drop_db(&source_admin, &source_db).await;
}
