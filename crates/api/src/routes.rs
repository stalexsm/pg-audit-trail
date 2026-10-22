//! Схема OpenAPI и сборка роутера API. Аннотации `#[utoipa::path]`
//! живут на обработчиках (`crate::handlers`) — одна точка правды
//! для сигнатур, роутинга и документации (тикет #54); врапперы
//! роутинга устранены.

use axum::{
    Router,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use sqlx::{Pool, Postgres};
use utoipa::OpenApi;

use crate::AppError;
use crate::dto::{
    ChangeDetailDTO, ChangePageDTO, ChangeRowDTO, QSourceCreate, QSourcePatch, SourceDTO,
};
use crate::handlers::{
    // Служебные типы, порождённые макросом `#[utoipa::path]` на
    // обработчиках: на них ссылается derive OpenApi ниже.
    __path_delete_source,
    __path_get_change_by_id,
    __path_get_list_changes,
    __path_get_list_sources,
    __path_get_source_by_name,
    __path_patch_source,
    __path_post_source,
    delete_source,
    get_change_by_id,
    get_list_changes,
    get_list_sources,
    get_source_by_name,
    patch_source,
    post_source,
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Waltrail — документация API журнала аудита",
        description = "Чтение журнала аудита: только изменения строк — единая сущность `changes` (ADR-0004). Управление реестром источников (ADR-0005): DELETE помечает источник к удалению (202), исполнение — за воркером; URL источника неизменяем. В ответах строка подключения маскируется (тикет #38): видны схема и хост, креды, порт и имя базы скрыты; полный URL передаётся только при регистрации. Значения по чувствительным ключам словаря (пароль и синонимы, включая вложенные объекты — ADR-0006, тикет #39) в образах «до/после» и контексте строк журнала маскируются одинаково и в списке, и в детали; в БД хранятся полные значения — фильтры по `context` работают по сохранённым данным."
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

/// Фолбэк роутера: маршрут не существует (404). Строится общим хелпером
/// `AppError::not_found`, как и 404 швов API, — единый JSON-формат
/// ответов 404 по всему приложению (тикет #54).
pub async fn not_found_fallback() -> Response {
    AppError::not_found("Данной страницы не существует. Проверьте правильность адреса...")
        .into_response()
}
