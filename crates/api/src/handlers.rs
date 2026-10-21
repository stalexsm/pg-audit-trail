use anyhow::anyhow;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
};
use axum_valid::Valid;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    AppError, AppResult,
    dto::{ChangeDetailDTO, ChangePageDTO, QChange},
    repositories::{ChangeRepo, JournalQuery},
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

    let query = JournalQuery {
        page: q.page,
        per_page: q.per_page,
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
