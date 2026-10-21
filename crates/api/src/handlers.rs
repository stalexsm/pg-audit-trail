use anyhow::anyhow;
use axum::{
    extract::{Query, State},
    http::StatusCode,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    AppError, AppResult,
    dto::{ChangeDetailDTO, ChangePageDTO, QChange},
    repositories::ChangeRepo,
};

/// Список журнала: изменения и события приложения с пагинацией,
/// сортировкой и фильтрами.
pub async fn get_list_changes(
    State(pool): State<PgPool>,
    Query(q): Query<QChange>,
) -> AppResult<ChangePageDTO> {
    let (rows, pages) = ChangeRepo::get_list_paginate(&pool, &q).await;

    Ok(ChangePageDTO {
        page: q.normalized_page(),
        pages,
        rows,
    })
}

/// Детальная строка журнала: полные образы «до/после» и контекст.
pub async fn get_change_by_id(
    State(pool): State<PgPool>,
    id: axum::extract::Path<Uuid>,
) -> AppResult<ChangeDetailDTO> {
    let res = ChangeRepo::get_by_id(&pool, *id).await;

    match res {
        Some(res) => Ok(res),
        None => Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow!("Данной записи не существует!"),
        )),
    }
}
