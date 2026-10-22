//! Обработчики API: аннотации `#[utoipa::path]` живут на самих функциях —
//! одна точка правды для сигнатур, роутинга и документации (тикет #54).
//! Функции сразу возвращают HTTP-форму ответа (`Json`, код статуса):
//! врапперы роутинга устранены.

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
/// Swagger. Значения по чувствительным ключам словаря (ADR-0006)
/// в сводке образов и контексте маскируются — так же, как в детали.
#[utoipa::path(
    get,
    path = "/api/changes",
    tag = "Changes",
    params(QChange),
    responses(
        (status = 200, description = "Страница журнала", body = ChangePageDTO),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 500, description = "Ошибка сервера: нейтральное сообщение и correlation id (детали — только в логе, тикет #45)")
    )
)]
pub async fn get_list_changes(
    State(pool): State<PgPool>,
    Valid(Query(q)): Valid<Query<QChange>>,
) -> AppResult<Json<ChangePageDTO>> {
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

    Ok(Json(ChangePageDTO {
        page: query.page,
        pages,
        rows,
    }))
}

/// Детальная строка журнала: полные образы «до/после» и контекст.
/// Значения по чувствительным ключам словаря (ADR-0006) в образах
/// и контексте маскируются — так же, как в списке. Ошибка БД — 500,
/// отсутствующая запись — 404.
#[utoipa::path(
    get,
    path = "/api/changes/{id}",
    tag = "Changes",
    responses(
        (status = 200, description = "Детальная строки журнала", body = ChangeDetailDTO),
        (status = 400, description = "Некорректные параметры запроса"),
        (status = 404, description = "Записи не существует"),
        (status = 500, description = "Ошибка сервера: нейтральное сообщение и correlation id (детали — только в логе, тикет #45)")
    )
)]
pub async fn get_change_by_id(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<ChangeDetailDTO>> {
    match ChangeRepo::get_by_id(&pool, id).await? {
        Some(res) => Ok(Json(res)),
        None => Err(AppError::not_found("Данной записи не существует!")),
    }
}

// === Реестр источников (ADR-0005, тикет #28) ===

/// Сообщение 404: источник с таким именем в реестре отсутствует.
const SOURCE_NOT_FOUND: &str = "Данного источника не существует";

/// Регистрирует источник: тело проверено на уровне axum (`Valid<Json<_>>`),
/// дубликат имени — 400, прочая ошибка БД — 500. Статус воркера нового
/// источника — дефолт схемы (`pending`); в ответе url маскирован
/// (тикет #38): видны схема и хост, креды, порт и имя базы скрыты.
#[utoipa::path(
    post,
    path = "/api/sources",
    tag = "Sources",
    request_body = QSourceCreate,
    responses(
        (status = 201, description = "Источник зарегистрирован", body = SourceDTO),
        (status = 400, description = "Ошибка валидации: невалидное имя, URL или дубликат имени"),
        (status = 500, description = "Ошибка сервера: нейтральное сообщение и correlation id (детали — только в логе, тикет #45)")
    )
)]
pub async fn post_source(
    State(pool): State<PgPool>,
    Valid(Json(q)): Valid<Json<QSourceCreate>>,
) -> AppResult<(StatusCode, Json<SourceDTO>)> {
    let source = SourceRepo::create(
        &pool,
        &SourceCreate {
            name: q.name,
            url: q.url,
            ignore_tables: q.ignore_tables,
            message_prefix: q.message_prefix,
            batch_size: q.batch_size,
        },
    )
    .await?;

    Ok((StatusCode::CREATED, Json(source)))
}

/// Список источников реестра со статусами воркеров, по имени.
#[utoipa::path(
    get,
    path = "/api/sources",
    tag = "Sources",
    responses(
        (status = 200, description = "Источники реестра, по имени", body = Vec<SourceDTO>),
        (status = 500, description = "Ошибка сервера: нейтральное сообщение и correlation id (детали — только в логе, тикет #45)")
    )
)]
pub async fn get_list_sources(State(pool): State<PgPool>) -> AppResult<Json<Vec<SourceDTO>>> {
    let sources = SourceRepo::get_list(&pool).await?;

    Ok(Json(sources))
}

/// Один источник реестра по имени; отсутствующий — 404.
#[utoipa::path(
    get,
    path = "/api/sources/{name}",
    tag = "Sources",
    params(("name" = String, Path, description = "Имя источника")),
    responses(
        (status = 200, description = "Источник реестра", body = SourceDTO),
        (status = 404, description = "Источника не существует"),
        (status = 500, description = "Ошибка сервера: нейтральное сообщение и correlation id (детали — только в логе, тикет #45)")
    )
)]
pub async fn get_source_by_name(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
) -> AppResult<Json<SourceDTO>> {
    SourceRepo::get_by_name(&pool, &name)
        .await?
        .map(Json)
        .ok_or_else(|| AppError::not_found(SOURCE_NOT_FOUND))
}

/// Правит настройки источника (только `ignore_tables`, `batch_size`,
/// `message_prefix`; URL неизменяем — ADR-0005). Попытка передать `url` —
/// явный 400 (тикет #54): правка целиком не применяется. Отсутствующий —
/// 404, помеченный к удалению — 409.
#[utoipa::path(
    patch,
    path = "/api/sources/{name}",
    tag = "Sources",
    params(("name" = String, Path, description = "Имя источника")),
    request_body = QSourcePatch,
    responses(
        (status = 200, description = "Источник после правки", body = SourceDTO),
        (status = 400, description = "Ошибка валидации тела, включая попытку сменить неизменяемый url"),
        (status = 404, description = "Источника не существует"),
        (status = 409, description = "Источник помечен к удалению"),
        (status = 500, description = "Ошибка сервера: нейтральное сообщение и correlation id (детали — только в логе, тикет #45)")
    )
)]
pub async fn patch_source(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
    Valid(Json(q)): Valid<Json<QSourcePatch>>,
) -> AppResult<Json<SourceDTO>> {
    // Проверка до обращения к БД: правка с url невалидна независимо
    // от существования источника.
    if q.url.is_some() {
        return Err(AppError::bad_request(
            "Поле url неизменяемо: URL источника задаётся при регистрации (ADR-0005) — удалите источник и создайте заново",
        ));
    }

    let patch = SourcePatch {
        ignore_tables: q.ignore_tables,
        message_prefix: q.message_prefix,
        batch_size: q.batch_size,
    };

    match SourceRepo::update(&pool, &name, &patch).await? {
        Some(row) => Ok(Json(row)),
        None => match SourceRepo::get_by_name(&pool, &name).await? {
            Some(_) => Err(AppError::conflict("Источник помечен к удалению")),
            None => Err(AppError::not_found(SOURCE_NOT_FOUND)),
        },
    }
}

/// Помечает источник к удалению; исполнение (дроп слота и публикации у
/// клиента, снятие строки реестра) — за воркером, поэтому 202 (ADR-0005).
/// Повторный вызов идемпотентен.
#[utoipa::path(
    delete,
    path = "/api/sources/{name}",
    tag = "Sources",
    params(("name" = String, Path, description = "Имя источника")),
    responses(
        (status = 202, description = "Источник помечен к удалению"),
        (status = 404, description = "Источника не существует"),
        (status = 500, description = "Ошибка сервера: нейтральное сообщение и correlation id (детали — только в логе, тикет #45)")
    )
)]
pub async fn delete_source(
    State(pool): State<PgPool>,
    Path(name): Path<String>,
) -> AppResult<StatusCode> {
    match SourceRepo::mark_deleted(&pool, &name).await? {
        Some(()) => Ok(StatusCode::ACCEPTED),
        None => Err(AppError::not_found(SOURCE_NOT_FOUND)),
    }
}
