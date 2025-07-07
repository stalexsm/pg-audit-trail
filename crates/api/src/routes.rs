use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    middleware,
    response::Response,
    routing::get,
};
use casbin::Enforcer;
use parking_lot::RwLock;
use sqlx::{PgPool, Pool, Postgres};
use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};
use uuid::Uuid;

use crate::dto::{AuditPaginateDTO, ChangeDetailDTO, ChangePaginateDTO, QAudit, QChange};
use crate::handlers as s;
use crate::{AppResult, check_authenticate};

#[derive(OpenApi)]
#[openapi(

    info(
        title = "BORiY! Документация по `Микросервису Аудита`.",
        description = "Документация по `Микросервису Аудита API`."
    ),
    modifiers(&SecurityAddon),
    paths(get_list_audits, get_list_changes, get_change_by_id, get_audit_body),
    components(schemas(AuditPaginateDTO, ChangeDetailDTO, ChangePaginateDTO))
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .get_or_insert_with(utoipa::openapi::Components::new);
        components.add_security_scheme(
            "api_jwt_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

/// Создание роутера для API
pub fn api_router(jwt_seckey: String, enforcer: Arc<RwLock<Enforcer>>) -> Router<Pool<Postgres>> {
    Router::new()
        .route("/aud/audits", get(get_list_audits))
        .route("/aud/audits/{id}/response-body", get(get_audit_body))
        .route("/aud/audits/{id}/changes", get(get_list_changes))
        .route("/aud/changes/{id}", get(get_change_by_id))
        .route_layer(middleware::from_fn_with_state(
            (jwt_seckey, enforcer),
            check_authenticate,
        ))
}

/// Метод для получения списка журнала действий пользователя.
#[utoipa::path(
    get,
    path = "/api/aud/audits",
    tag = "Audits",
    params(QAudit),
    responses(
        (status = 200, description = "Список журнала действий пользователя", body = AuditPaginateDTO),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 403, description = "Нет доступа к данному методу"),
        (status = 500, description = "Ошибка сервера")
    ),
    security(
        ("api_jwt_token" = [])
    )
)]
pub async fn get_list_audits(
    State(pool): State<PgPool>,

    Query(q): Query<QAudit>,
) -> AppResult<Json<AuditPaginateDTO>> {
    let res = s::get_list_audits(State(pool), Query(q)).await?;

    Ok(Json(res))
}

/// Метод для получения response_body для записи.
#[utoipa::path(
    get,
    path = "/api/aud/audits/{id}/response-body",
    tag = "Audits",
    responses(
        (
            status = 200,
            description = "Данные для скачивания файла",
            content_type = "application/octet-stream",
            body = String,
            example = "Binary file data"
        ),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 403, description = "Нет доступа к данному методу"),
        (status = 404, description = "Записи не существует"),
        (status = 500, description = "Ошибка сервера")
    ),
    security(
        ("api_jwt_token" = [])
    )
)]
pub async fn get_audit_body(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> AppResult<Response> {
    let res = s::get_audit_body(State(pool), Path(id)).await?;

    Ok(res)
}

/// Метод для получения списка изменений.
#[utoipa::path(
    get,
    path = "/api/aud/audits/{id}/changes",
    tag = "Audits",
    params(QChange),
    responses(
        (status = 200, description = "Список изменений", body = ChangePaginateDTO),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 403, description = "Нет доступа к данному методу"),
        (status = 500, description = "Ошибка сервера")
    ),
    security(
        ("api_jwt_token" = [])
    )
)]
pub async fn get_list_changes(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
    Query(q): Query<QChange>,
) -> AppResult<Json<ChangePaginateDTO>> {
    let res = s::get_list_changes(State(pool), Path(id), Query(q)).await?;

    Ok(Json(res))
}

/// Метод для получения детпльной изменений.
#[utoipa::path(
    get,
    path = "/api/aud/changes/{id}",
    tag = "Audits",
    responses(
        (status = 200, description = "Детальная по изменениям", body = ChangeDetailDTO),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 403, description = "Нет доступа к данному методу"),
        (status = 404, description = "Записи не существует"),
        (status = 500, description = "Ошибка сервера")
    ),
    security(
        ("api_jwt_token" = [])
    )
)]
pub async fn get_change_by_id(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<ChangeDetailDTO>> {
    let res = s::get_change_by_id(State(pool), Path(id)).await?;

    Ok(Json(res))
}
