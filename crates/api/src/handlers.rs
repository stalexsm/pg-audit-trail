use anyhow::anyhow;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use axum_valid::Valid;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    AppError, AppResult,
    dto::{ChangeDetailDTO, ChangePageDTO, QChange, QSourceCreate, QSourcePatch, SourceDTO},
    repositories::{ChangeQuery, ChangeRepo, SourceCreate, SourcePatch, SourceRepo},
};

/// Список журнала: изменения строк источника с пагинацией,
/// сортировкой и фильтрами. Параметры проверены на уровне axum
/// (`Valid<Query<_>>`: нарушение — 400); ошибка БД — 500 по контракту
/// Swagger.
pub async fn get_list_changes(
    State(pool): State<PgPool>,
    Valid(Query(q)): Valid<Query<QChange>>,
) -> AppResult<ChangePageDTO> {
    let time_range = q.time_range()?;
    let descending = q.descending();

    // Пустое значение параметра — отсутствие фильтра: `?source=` не должен
    // случайно спрятать все именованные источники или стать выборкой
    // «только безымянные».
    let source = q.source.filter(|s| !s.is_empty());

    let query = ChangeQuery {
        page: q.page,
        per_page: q.per_page,
        source,
        trace_id: q.trace_id,
        table: q.table,
        operation: q.operation,
        time_range,
        descending,
        context_filters: q.context_filters,
    };

    let (rows, pages) = ChangeRepo::get_list_paginate(&pool, &query).await?;

    Ok(ChangePageDTO {
        page: query.page,
        pages,
        rows,
    })
}

/// Детальная строка журнала: полные образы «до/после» и контекст.
/// Ошибка БД — 500, отсутствующая запись — 404.
pub async fn get_change_by_id(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> AppResult<ChangeDetailDTO> {
    match ChangeRepo::get_by_id(&pool, id).await? {
        Some(res) => Ok(res),
        None => Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow!("Данной записи не существует!"),
        )),
    }
}

// === Реестр источников (ADR-0005, тикет #28) ===

/// Сообщение 404: источник с таким именем в реестре отсутствует.
const SOURCE_NOT_FOUND: &str = "Данного источника не существует";

/// Регистрирует источник: тело проверено на уровне axum (`Valid<Json<_>>`),
/// дубликат имени — 400, прочая ошибка БД — 500. Статус воркера нового
/// источника — дефолт схемы (`pending`).
pub async fn post_source(
    State(pool): State<PgPool>,
    Valid(Json(q)): Valid<Json<QSourceCreate>>,
) -> AppResult<SourceDTO> {
    SourceRepo::create(
        &pool,
        &SourceCreate {
            name: q.name,
            url: q.url,
            ignore_tables: q.ignore_tables,
            message_prefix: q.message_prefix,
            batch_size: q.batch_size,
        },
    )
    .await
}

/// Список источников реестра со статусами воркеров, по имени.
pub async fn get_list_sources(State(pool): State<PgPool>) -> AppResult<Vec<SourceDTO>> {
    SourceRepo::get_list(&pool).await
}

/// Один источник реестра по имени; отсутствующий — 404.
pub async fn get_source_by_name(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
) -> AppResult<SourceDTO> {
    SourceRepo::get_by_name(&pool, &name)
        .await?
        .ok_or_else(|| AppError::not_found(SOURCE_NOT_FOUND))
}

/// Правит настройки источника (только `ignore_tables`, `batch_size`,
/// `message_prefix`; URL неизменяем — ADR-0005). Отсутствующий — 404,
/// помеченный к удалению — 409.
pub async fn patch_source(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
    Valid(Json(q)): Valid<Json<QSourcePatch>>,
) -> AppResult<SourceDTO> {
    let patch = SourcePatch {
        ignore_tables: q.ignore_tables,
        message_prefix: q.message_prefix,
        batch_size: q.batch_size,
    };

    match SourceRepo::update(&pool, &name, &patch).await? {
        Some(row) => Ok(row),
        None => match SourceRepo::get_by_name(&pool, &name).await? {
            Some(_) => Err(AppError::conflict("Источник помечен к удалению")),
            None => Err(AppError::not_found(SOURCE_NOT_FOUND)),
        },
    }
}

/// Помечает источник к удалению; исполнение (дроп слота и публикации у
/// клиента, снятие строки реестра) — за воркером, поэтому 202 (ADR-0005).
pub async fn delete_source(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
) -> AppResult<StatusCode> {
    match SourceRepo::mark_deleted(&pool, &name).await? {
        Some(()) => Ok(StatusCode::ACCEPTED),
        None => Err(AppError::not_found(SOURCE_NOT_FOUND)),
    }
}
