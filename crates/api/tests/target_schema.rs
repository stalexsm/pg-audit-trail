//! Интеграционные тесты целевой схемы журнала (issue #5, Waltrail 02;
//! contract-снос мёртвых объектов — issue #11, Waltrail 08).
//!
//! Требуют поднятой инфраструктуры: `docker compose up -d`.
//! Подключение: `TEST_DATABASE_URL` (по умолчанию — `db1` с проброшенным портом).

use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// URL администраторского подключения к тестовой БД аудита.
fn admin_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:54399/postgres".to_string())
}

/// Поднимает временную БД с уникальным именем, применяет миграции, отдаёт пул к ней.
/// Возвращает (пул, имя БД) — вызывающий обязан вызвать `drop_test_db`.
async fn spawn_migrated_db(admin: &PgPool) -> (PgPool, String) {
    let db_name = format!("waltrail_schema_test_{}", uuid::Uuid::new_v4().simple());

    // Имя БД генерируется из UUID здесь же — инъекции нет (аудит).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{db_name}\""
    )))
    .execute(admin)
    .await
    .expect("не удалось создать временную тестовую БД");

    let base_url = admin_url();
    let (prefix, _) = base_url
        .rsplit_once('/')
        .expect("TEST_DATABASE_URL должен содержать путь к БД");
    let db_url = format!("{prefix}/{db_name}");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("не удалось подключиться к временной тестовой БД");

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("миграции должны применяться на свежей БД");

    (pool, db_name)
}

async fn drop_test_db(admin: &PgPool, db_name: &str) {
    // Имя БД — из фикстуры этого же теста, без пользовательского ввода (аудит).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
    )))
    .execute(admin)
    .await
    .expect("не удалось удалить временную тестовую БД");
}

/// Ожидаемые колонки `changes` из целевой схемы: (имя, тип, допускает NULL).
const TARGET_COLUMNS: &[(&str, &str, &str)] = &[
    ("commit_lsn", "bigint", "YES"),
    ("event_index", "integer", "YES"),
    ("trace_id", "character varying", "YES"),
    // Ключ источника (ADR-0005, тикет #29): пустая строка — источник
    // неизвестен (строки до переезда на мультиисточность).
    ("source", "character varying", "NO"),
    // Колонка сохраняет смысл «LSN события»
    ("position", "bigint", "NO"),
];

#[tokio::test]
async fn target_changes_schema_is_applied() {
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_url())
        .await
        .expect("нет подключения к тестовому Postgres — поднимите `docker compose up -d`");

    let (pool, db_name) = spawn_migrated_db(&admin).await;

    // Целевые колонки `changes` на месте с ожидаемыми типами и nullability.
    for (name, data_type, nullable) in TARGET_COLUMNS {
        let row = sqlx::query(
            "SELECT data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = 'changes' AND column_name = $1",
        )
        .bind(name)
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|e| panic!("колонка changes.{name} отсутствует: {e}"));

        assert_eq!(row.get::<String, _>("data_type"), *data_type, "{name}: тип");
        assert_eq!(
            row.get::<String, _>("is_nullable"),
            *nullable,
            "{name}: nullability"
        );
    }

    // Полный состав схемы — white-list: объекты создаются только миграциями,
    // мёртвых таблиц и колонок в целевой схеме нет (issue #11, Waltrail 08).
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' \
         AND tablename != '_sqlx_migrations' ORDER BY tablename",
    )
    .fetch_all(&pool)
    .await
    .expect("список таблиц схемы public");
    assert_eq!(
        tables,
        vec![
            "changes".to_string(),
            "checkpoints".to_string(),
            "sources".to_string(),
        ],
        "в public должны быть только таблицы журнала, чекпоинтов и реестра источников (кроме служебной _sqlx_migrations)"
    );

    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'changes' \
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("список колонок changes");
    assert_eq!(
        columns,
        vec![
            "id",
            "database",
            "schema",
            "table",
            "primary_key",
            "operation",
            "before",
            "after",
            "context",
            "committed_at",
            "queued_at",
            "created_at",
            "transaction_id",
            "position",
            "commit_lsn",
            "event_index",
            "trace_id",
            "source",
        ],
        "неожиданный состав колонок changes"
    );

    // Btree-индекс по trace_id.
    let trace_id_index: String = sqlx::query_scalar(
        "SELECT indexdef \
         FROM pg_indexes \
         WHERE schemaname = 'public' AND tablename = 'changes' \
           AND indexname = 'changes_trace_id_index'",
    )
    .fetch_one(&pool)
    .await
    .expect("индекс changes_trace_id_index не найден");
    assert!(
        trace_id_index.to_uppercase().contains("USING BTREE")
            && trace_id_index.to_uppercase().contains("TRACE_ID"),
        "changes_trace_id_index должен быть btree по trace_id: {trace_id_index}"
    );

    // Btree-индекс по ключу источника: фильтр чтения журнала — равенство.
    let source_index: String = sqlx::query_scalar(
        "SELECT indexdef \
         FROM pg_indexes \
         WHERE schemaname = 'public' AND tablename = 'changes' \
           AND indexname = 'changes_source_index'",
    )
    .fetch_one(&pool)
    .await
    .expect("индекс changes_source_index не найден");
    assert!(
        source_index.to_uppercase().contains("USING BTREE")
            && source_index.to_uppercase().contains("SOURCE"),
        "changes_source_index должен быть btree по source: {source_index}"
    );

    // Реестр источников (ADR-0005, тикет #28): состав колонок и правило
    // имён слотов на уровне схемы.
    let source_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'sources' \
         ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .expect("список колонок sources");
    assert_eq!(
        source_columns,
        vec![
            "name",
            "url",
            "ignore_tables",
            "message_prefix",
            "batch_size",
            "worker_state",
            "last_event_at",
            "last_error",
            "deleted_at",
            "created_at",
            "updated_at",
        ],
        "неожиданный состав колонок sources"
    );

    let slot_rule: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) \
         FROM pg_constraint \
         WHERE conrelid = 'public.sources'::regclass \
           AND conname = 'sources_name_slot_rule'",
    )
    .fetch_one(&pool)
    .await
    .expect("констрейнт sources_name_slot_rule не найден");
    assert!(
        slot_rule.contains("^[a-z0-9_]+$"),
        "правило имён слотов в схеме: {slot_rule}"
    );

    // Уникальные констрейнты журнала включают ключ источника (ADR-0005):
    // LSN и txid разных серверов коллидируют, без ключа вставка второго
    // источника падает.
    let constraint_def: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) \
         FROM pg_constraint \
         WHERE conrelid = 'public.changes'::regclass \
           AND conname = 'changes_commit_lsn_event_index_unique'",
    )
    .fetch_one(&pool)
    .await
    .expect("констрейнт changes_commit_lsn_event_index_unique не найден");
    assert_eq!(
        constraint_def.replace(' ', "").to_lowercase(),
        "unique(source,commit_lsn,event_index)",
        "неожиданное определение констрейнта дедупликации: {constraint_def}"
    );

    let position_def: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) \
         FROM pg_constraint \
         WHERE conrelid = 'public.changes'::regclass \
           AND conname = 'changes_position_table_schema_database_operation_unique'",
    )
    .fetch_one(&pool)
    .await
    .expect("констрейнт changes_position_table_schema_database_operation_unique не найден");
    assert_eq!(
        position_def.replace(' ', "").to_lowercase(),
        "unique(source,\"position\",\"table\",schema,database,operation)",
        "неожиданное определение констрейнта позиции: {position_def}"
    );

    // Функционально: replay-дубль по дедуп-ключу гасится, а строки старого
    // пайплайна (NULL, NULL) не конфликтуют между собой.
    let insert = |commit_lsn: Option<i64>, event_index: Option<i32>, position: i64| {
        sqlx::query(
            "INSERT INTO changes \
             (database, schema, \"table\", operation, before, after, context, \
              transaction_id, position, commit_lsn, event_index) \
             VALUES ('db', 'public', 't', 'CREATE', '{}', '{}', '{}', 1, $1, $2, $3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(position)
        .bind(commit_lsn)
        .bind(event_index)
    };

    let inserted = insert(Some(100), Some(0), 1)
        .execute(&pool)
        .await
        .expect("вставка первой строки")
        .rows_affected();
    assert_eq!(inserted, 1, "первая строка должна вставиться");

    let replayed = insert(Some(100), Some(0), 2)
        .execute(&pool)
        .await
        .expect("вставка replay-дубля")
        .rows_affected();
    assert_eq!(replayed, 0, "replay-дубль должен быть погашен дедуп-ключом");

    let legacy_first = insert(None, None, 3)
        .execute(&pool)
        .await
        .expect("вставка строки старого пайплайна")
        .rows_affected();
    let legacy_second = insert(None, None, 4)
        .execute(&pool)
        .await
        .expect("вставка второй строки старого пайплайна")
        .rows_affected();
    assert_eq!(
        legacy_first + legacy_second,
        2,
        "строки с (NULL, NULL) не должны конфликтовать по дедуп-ключу"
    );

    // Одинаковый дедуп-ключ у разных источников не конфликтует: строки
    // разводит ключ источника (ADR-0005, тикет #29), как при вставке
    // изменений двух серверов с коллидирующими LSN.
    let other_source = sqlx::query(
        "INSERT INTO changes \
         (source, database, schema, \"table\", operation, before, after, context, \
          transaction_id, position, commit_lsn, event_index) \
         VALUES ('client_b', 'db', 'public', 't', 'CREATE', '{}', '{}', '{}', 1, 5, 100, 0) \
         ON CONFLICT DO NOTHING",
    )
    .execute(&pool)
    .await
    .expect("вставка строки второго источника")
    .rows_affected();
    assert_eq!(
        other_source, 1,
        "строка другого источника с тем же дедуп-ключом должна вставиться"
    );
    let other_source_replay = sqlx::query(
        "INSERT INTO changes \
         (source, database, schema, \"table\", operation, before, after, context, \
          transaction_id, position, commit_lsn, event_index) \
         VALUES ('client_b', 'db', 'public', 't', 'CREATE', '{}', '{}', '{}', 1, 5, 100, 0) \
         ON CONFLICT DO NOTHING",
    )
    .execute(&pool)
    .await
    .expect("повторная вставка строки второго источника")
    .rows_affected();
    assert_eq!(
        other_source_replay, 0,
        "replay-дубль второго источника гасится своим дедуп-ключом"
    );

    // История миграций чистая: полный цикл down/up проходит на той же БД.
    // Пробные строки двух источников убираются: на смешанных по источникам
    // данных откат невозможен по определению — старые констрейнты не
    // разводят совпадающие LSN (см. комментарий в down-миграции), а молча
    // удалять строки журнала откат не имеет права.
    sqlx::query("DELETE FROM changes")
        .execute(&pool)
        .await
        .expect("очистка пробных строк перед откатом миграций");
    let migrator = sqlx::migrate!("../../migrations");
    migrator
        .undo(&pool, 0)
        .await
        .expect("обратные миграции должны откатывать все изменения");
    let changes_gone: bool = sqlx::query_scalar("SELECT to_regclass('public.changes') IS NULL")
        .fetch_one(&pool)
        .await
        .expect("запрос отсутствия changes после down");
    assert!(changes_gone, "после down таблица changes должна исчезнуть");

    migrator
        .run(&pool)
        .await
        .expect("повторное применение миграций на той же БД");

    drop_test_db(&admin, &db_name).await;
}
