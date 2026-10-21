use anyhow::Context;
use pgwire_replication::Lsn;
use sqlx::PgPool;
use tracing::info;

use crate::Result;

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

/// Убеждается в существовании слота репликации и читает точку старта.
///
/// Источник истины — чекпоинт в БД аудита; при старте берётся меньшая из
/// координат (чекпоинт, `confirmed_flush_lsn` слота), чтобы после сбоя
/// и/или лага слота не потерять события.
pub async fn bootstrap(audit: &PgPool, source: &PgPool, slot: &str) -> Result<Bootstrap> {
    let (source_database, version_num): (String, i32) =
        sqlx::query_as("SELECT current_database(), current_setting('server_version_num')::int")
            .fetch_one(source)
            .await
            .context("параметры источника CDC")?;

    // Слот создаётся, только если его ещё нет: волатильная функция в списке
    // выборки не вычисляется для отфильтрованных WHERE строк.
    let created: Option<(String, String)> = sqlx::query_as(
        "SELECT lsn::text, slot_name::text \
         FROM pg_create_logical_replication_slot($1, 'pgoutput') \
         WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
    )
    .bind(slot)
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
    .bind(slot)
    .fetch_optional(source)
    .await
    .context("чтение confirmed_flush_lsn слота")?;

    let checkpoint: Option<i64> =
        sqlx::query_scalar("SELECT last_commit_lsn FROM checkpoints WHERE name = $1")
            .bind(slot)
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
