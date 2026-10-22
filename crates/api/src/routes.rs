use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use axum_valid::Valid;
use sqlx::{PgPool, Pool, Postgres};
use utoipa::OpenApi;
use uuid::Uuid;

use crate::AppResult;
use crate::dto::{
    ChangeDetailDTO, ChangePageDTO, ChangeRowDTO, QChange, QSourceCreate, QSourcePatch, SourceDTO,
};
use crate::handlers as s;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Waltrail — документация API журнала аудита",
        description = "Чтение журнала аудита: только изменения строк — единая сущность `changes` (ADR-0004). Управление реестром источников (ADR-0005): DELETE помечает источник к удалению (202), исполнение — за воркером; URL источника неизменяем. В ответах строка подключения маскируется (тикет #38): видны схема и хост, креды, порт и имя базы скрыты; полный URL передаётся только при регистрации."
    ),
    paths(
        get_list_changes,
        get_change_by_id,
        post_source,
        get_list_sources,
        get_source_by_name,
        patch_source,
        delete_source
    ),
    components(schemas(
        ChangePageDTO,
        ChangeRowDTO,
        ChangeDetailDTO,
        SourceDTO,
        QSourceCreate,
        QSourcePatch
    ))
)]
pub struct ApiDoc;

/// Роутер API: роутер сам владеет префиксом `/api`.
pub fn api_router() -> Router<Pool<Postgres>> {
    Router::new()
        .route("/api/changes", get(get_list_changes))
        .route("/api/changes/{id}", get(get_change_by_id))
        .route("/api/sources", post(post_source).get(get_list_sources))
        .route(
            "/api/sources/{name}",
            get(get_source_by_name)
                .patch(patch_source)
                .delete(delete_source),
        )
}

/// Метод для получения списка журнала: изменения строк источника.
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

/// Регистрирует источник журнала (ADR-0005); в ответе url маскирован
/// (тикет #38): видны схема и хост, креды, порт и имя базы скрыты.
#[utoipa::path(
    post,
    path = "/api/sources",
    tag = "Sources",
    request_body = QSourceCreate,
    responses(
        (status = 201, description = "Источник зарегистрирован", body = SourceDTO),
        (status = 400, description = "Ошибка валидации: невалидное имя, URL или дубликат имени"),
        (status = 500, description = "Ошибка сервера")
    )
)]
pub async fn post_source(
    State(pool): State<PgPool>,
    Valid(Json(q)): Valid<Json<QSourceCreate>>,
) -> AppResult<(StatusCode, Json<SourceDTO>)> {
    let res = s::post_source(State(pool), Valid(Json(q))).await?;

    Ok((StatusCode::CREATED, Json(res)))
}

/// Список источников реестра со статусами воркеров.
#[utoipa::path(
    get,
    path = "/api/sources",
    tag = "Sources",
    responses(
        (status = 200, description = "Источники реестра, по имени", body = Vec<SourceDTO>),
        (status = 500, description = "Ошибка сервера")
    )
)]
pub async fn get_list_sources(State(pool): State<PgPool>) -> AppResult<Json<Vec<SourceDTO>>> {
    let res = s::get_list_sources(State(pool)).await?;

    Ok(Json(res))
}

/// Один источник реестра по имени.
#[utoipa::path(
    get,
    path = "/api/sources/{name}",
    tag = "Sources",
    params(("name" = String, Path, description = "Имя источника")),
    responses(
        (status = 200, description = "Источник реестра", body = SourceDTO),
        (status = 404, description = "Источника не существует"),
        (status = 500, description = "Ошибка сервера")
    )
)]
pub async fn get_source_by_name(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
) -> AppResult<Json<SourceDTO>> {
    let res = s::get_source_by_name(State(pool), Path(name)).await?;

    Ok(Json(res))
}

/// Правит настройки источника: только `ignore_tables`, `message_prefix`,
/// `batch_size`; URL неизменяем (ADR-0005). Опущенное поле остаётся без изменений.
#[utoipa::path(
    patch,
    path = "/api/sources/{name}",
    tag = "Sources",
    params(("name" = String, Path, description = "Имя источника")),
    request_body = QSourcePatch,
    responses(
        (status = 200, description = "Источник после правки", body = SourceDTO),
        (status = 400, description = "Ошибка валидации тела"),
        (status = 404, description = "Источника не существует"),
        (status = 409, description = "Источник помечен к удалению"),
        (status = 500, description = "Ошибка сервера")
    )
)]
pub async fn patch_source(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
    Valid(Json(q)): Valid<Json<QSourcePatch>>,
) -> AppResult<Json<SourceDTO>> {
    let res = s::patch_source(State(pool), Path(name), Valid(Json(q))).await?;

    Ok(Json(res))
}

/// Помечает источник к удалению; исполнение — за воркером (ADR-0005),
/// поэтому 202. Повторный вызов идемпотентен.
#[utoipa::path(
    delete,
    path = "/api/sources/{name}",
    tag = "Sources",
    params(("name" = String, Path, description = "Имя источника")),
    responses(
        (status = 202, description = "Источник помечен к удалению"),
        (status = 404, description = "Источника не существует"),
        (status = 500, description = "Ошибка сервера")
    )
)]
pub async fn delete_source(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
) -> AppResult<StatusCode> {
    s::delete_source(State(pool), Path(name)).await
}
