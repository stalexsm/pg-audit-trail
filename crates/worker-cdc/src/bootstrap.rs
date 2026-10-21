use anyhow::Context;
use pgwire_replication::Lsn;
use sqlx::PgPool;
use tracing::info;

use crate::Result;
use crate::registry::SourceRecord;

/// Результат бутстрапа: данные для запуска репликации.
pub struct Bootstrap {
    /// Имя базы источника — значение колонки `database` журнала.
    pub source_database: String,
    /// Позиция старта репликации: min(чекпоинт, `confirmed_flush_lsn` слота).
    /// Меньшая из координат гарантирует повторную доставку уже сохранённого,
    /// лишний replay гасится дедуп-ключом `(commit_lsn, event_index)`.
    pub start_lsn: Lsn,
    /// Старшая версия сервера источника: в PG 17+ сообщение Relation
    /// дополнено байтом replica identity.
    pub server_major: u32,
}

/// Бутстрап при подхвате источника (ADR-0005): только воркер ходит в
/// клиентскую БД, поэтому публикация и слот создаются им же. Источник и
/// истина координаты старта — чекпоинт в БД аудита; при старте берётся
/// меньшая из координат (чекпоинт, `confirmed_flush_lsn` слота), чтобы
/// после сбоя и/или лага слота не потерять события.
pub async fn bootstrap(
    audit: &PgPool,
    source: &PgPool,
    record: &SourceRecord,
) -> Result<Bootstrap> {
    let publication = record.publication();
    let slot = record.slot();
    let (source_database, version_num): (String, i32) =
        sqlx::query_as("SELECT current_database(), current_setting('server_version_num')::int")
            .fetch_one(source)
            .await
            .context("параметры источника CDC")?;

    // Публикация создаётся при подхвате (ADR-0005): FOR ALL TABLES, чтобы
    // новые таблицы клиента попадали в поток без правок; фильтрация
    // служебных таблиц — на стороне воркера (игнор-лист источника).
    let publication_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)")
            .bind(&publication)
            .fetch_one(source)
            .await
            .context("проверка публикации источника")?;
    if publication_exists {
        info!(publication, "публикация уже существует");
    } else {
        // Имя публикации деривировано из имени источника, проверенного
        // правилом слотов [a-z0-9_] (CHECK в схеме реестра) — инъекции нет.
        let create = sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE PUBLICATION \"{publication}\" FOR ALL TABLES"
        )));
        match create.execute(source).await {
            Ok(_) => info!(publication, "публикация создана"),
            // Повторный бутстрап после падения между проверкой и созданием:
            // существование публикации — успех.
            Err(e) if is_duplicate_object(&e) => {
                info!(publication, "публикация уже создана параллельно")
            }
            Err(e) => return Err(anyhow::Error::new(e).context("создание публикации")),
        }
    }

    // Слот создаётся, только если его ещё нет: волатильная функция в списке
    // выборки не вычисляется для отфильтрованных WHERE строк.
    let created: Option<(String, String)> = sqlx::query_as(
        "SELECT lsn::text, slot_name::text \
         FROM pg_create_logical_replication_slot($1, 'pgoutput') \
         WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
    )
    .bind(&slot)
    .fetch_optional(source)
    .await
    .context("создание слота репликации")?;
    if created.is_some() {
        info!(slot, "слот репликации создан");
    } else {
        info!(slot, "слот репликации уже существует");
    }

    let confirmed: Option<String> = sqlx::query_scalar(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(&slot)
    .fetch_optional(source)
    .await
    .context("чтение confirmed_flush_lsn слота")?;

    let checkpoint: Option<i64> =
        sqlx::query_scalar("SELECT last_commit_lsn FROM checkpoints WHERE name = $1")
            .bind(&slot)
            .fetch_optional(audit)
            .await
            .context("чтение чекпоинта")?;

    let confirmed_lsn = confirmed
        .map(|text| {
            Lsn::parse(&text)
                .map(|lsn| lsn.as_u64())
                .map_err(|e| anyhow::anyhow!("не удалось разобрать LSN слота {text}: {e}"))
        })
        .transpose()?;

    let start_lsn = match (checkpoint, confirmed_lsn) {
        (Some(checkpoint), Some(confirmed)) => Lsn::from_u64((checkpoint as u64).min(confirmed)),
        (Some(checkpoint), None) => Lsn::from_u64(checkpoint as u64),
        (None, Some(confirmed)) => Lsn::from_u64(confirmed),
        // Слот только что создан — сервер сам начнёт с его точки согласованности.
        (None, None) => Lsn::ZERO,
    };

    info!(
        slot,
        checkpoint,
        ?confirmed_lsn,
        start_lsn = %start_lsn,
        "точка старта репликации"
    );

    Ok(Bootstrap {
        source_database,
        start_lsn,
        server_major: u32::try_from(version_num / 10_000).unwrap_or(17),
    })
}

/// Объект уже существует (42710 duplicate_object у `CREATE PUBLICATION`,
/// 42P07 duplicate_table у прочих объектов) — гонка с параллельным
/// созданием того же объекта: считаем существование успехом.
fn is_duplicate_object(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code.as_ref() == "42710" || code.as_ref() == "42P07")
}

/// Имена колонок первичного ключа таблицы источника по OID отношения.
///
/// Нужен как фолбэк для `REPLICA IDENTITY FULL`: при ней key-флаги
/// сообщения Relation помечают все колонки и ключ по ним не определить.
/// Пустой вектор — первичного ключа нет.
pub async fn primary_key_columns(source: &PgPool, relation_oid: u32) -> Result<Vec<String>> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey::int2[]) \
         WHERE i.indrelid = $1 AND i.indisprimary \
         ORDER BY array_position(i.indkey::int2[], a.attnum)",
    )
    .bind(i32::try_from(relation_oid).context("OID отношения не влезает в i32")?)
    .fetch_all(source)
    .await
    .context("чтение первичного ключа из каталога источника")?;
    Ok(names)
}
