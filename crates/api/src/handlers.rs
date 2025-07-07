use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Response,
};
use hyper::header::{ACCESS_CONTROL_EXPOSE_HEADERS, CONTENT_DISPOSITION, CONTENT_TYPE};
use sqlx::PgPool;

use uuid::Uuid;

use crate::{
    AppError, AppResult,
    dto::{AuditPaginateDTO, ChangeDetailDTO, ChangePaginateDTO, QAudit, QChange},
    repositories::{AuditRepo, ChangeRepo},
};
use base64::prelude::*;

// Для получения списка данных журнала
pub async fn get_list_audits(
    State(pool): State<PgPool>,
    Query(q): Query<QAudit>,
) -> AppResult<AuditPaginateDTO> {
    let page = q.page;
    let (rows, pages) = AuditRepo::get_list_paginate(State(pool), Query(q)).await;

    Ok(AuditPaginateDTO { page, pages, rows })
}

// Для получения response_body для записи журнала
pub async fn get_audit_body(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> AppResult<Response> {
    // Получаем содержимое аудита из репозитория
    let response_body = AuditRepo::get_body(State(pool), Path(id))
        .await
        .ok_or_else(|| {
            AppError(
                StatusCode::NOT_FOUND,
                anyhow!("Данной записи не существует!"),
            )
        })?;

    // Пытаемся декодировать содержимое из base64
    let decoded_result = BASE64_STANDARD.decode(&response_body);

    match decoded_result {
        Ok(buf) => {
            // Определяем тип контента и имя файла на основе содержимого
            let (content_type, filename) = detect_content_type(&buf, id);

            // Строим HTTP-ответ с нужными заголовками
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, content_type)
                .header(
                    CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", filename),
                )
                .header(ACCESS_CONTROL_EXPOSE_HEADERS, CONTENT_DISPOSITION)
                .body(Body::from(buf))
                .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, anyhow!(e)))
        }
        Err(_) => {
            // Если не удалось декодировать, отдаем как обычный текст
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/plain")
                .header(
                    CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}.txt\"", id),
                )
                .header(ACCESS_CONTROL_EXPOSE_HEADERS, CONTENT_DISPOSITION)
                .body(Body::from(response_body.into_bytes()))
                .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, anyhow!(e)))
        }
    }
}

// Вспомогательная функция для определения типа контента
fn detect_content_type(buf: &[u8], id: Uuid) -> (&'static str, String) {
    if let Some(kind) = infer::get(buf) {
        // Если удалось определить тип файла
        (kind.mime_type(), format!("{}.{}", id, kind.extension()))
    } else if serde_json::from_slice::<serde_json::Value>(buf).is_ok() {
        // Если это JSON
        ("application/json", format!("{}.json", id))
    } else {
        // По умолчанию - текст
        ("text/plain", format!("{}.txt", id))
    }
}

// Для получения изменений по для записи журнала
pub async fn get_list_changes(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
    Query(q): Query<QChange>,
) -> AppResult<ChangePaginateDTO> {
    let offset = q.offset;

    let (rows, cnt) = ChangeRepo::get_list_paginate(State(pool), Path(id), Query(q)).await;

    Ok(ChangePaginateDTO { offset, cnt, rows })
}

// Для получения детальной изменений
pub async fn get_change_by_id(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> AppResult<ChangeDetailDTO> {
    let res = ChangeRepo::get_by_id(State(pool), Path(id)).await;

    if let Some(res) = res {
        Ok(res)
    } else {
        Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow!("Данной записи не существует!"),
        ))
    }
}
