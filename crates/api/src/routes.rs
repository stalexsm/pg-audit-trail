use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use axum_valid::Valid;
use sqlx::{PgPool, Pool, Postgres};
use utoipa::OpenApi;
use uuid::Uuid;

use crate::AppResult;
use crate::dto::{ChangeDetailDTO, ChangePageDTO, ChangeRowDTO, QChange};
use crate::handlers as s;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Waltrail — документация API журнала аудита",
        description = "Чтение журнала аудита: изменения данных и события приложения — единая сущность `changes` (ADR-0002)."
    ),
    paths(get_list_changes, get_change_by_id),
    components(schemas(ChangePageDTO, ChangeRowDTO, ChangeDetailDTO))
)]
pub struct ApiDoc;

/// Роутер API: роутер сам владеет префиксом `/api`.
pub fn api_router() -> Router<Pool<Postgres>> {
    Router::new()
        .route("/api/changes", get(get_list_changes))
        .route("/api/changes/{id}", get(get_change_by_id))
}

/// Метод для получения списка журнала: изменения и события приложения.
#[utoipa::path(
    get,
    path = "/api/changes",
    tag = "Changes",
    params(QChange),
    responses(
        (status = 200, description = "Страница журнала", body = ChangePageDTO),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 500, description = "Ошибка сервера")
    )
)]
pub async fn get_list_changes(
    State(pool): State<PgPool>,
    Valid(Query(q)): Valid<Query<QChange>>,
) -> AppResult<Json<ChangePageDTO>> {
    let res = s::get_list_changes(State(pool), Valid(Query(q))).await?;

    Ok(Json(res))
}

/// Метод для получения детальной строки журнала.
#[utoipa::path(
    get,
    path = "/api/changes/{id}",
    tag = "Changes",
    responses(
        (status = 200, description = "Детальная строки журнала", body = ChangeDetailDTO),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 404, description = "Записи не существует"),
        (status = 500, description = "Ошибка сервера")
    )
)]
pub async fn get_change_by_id(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<ChangeDetailDTO>> {
    let res = s::get_change_by_id(State(pool), Path(id)).await?;

    Ok(Json(res))
}
