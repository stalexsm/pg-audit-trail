use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub mod dto;
pub mod handlers;
pub mod repositories;
pub mod routes;

pub type AppResult<T, E = AppError> = core::result::Result<T, E>;

#[derive(Debug)]
pub struct AppError(StatusCode, anyhow::Error);

impl AppError {
    /// Ошибка запроса клиента: некорректные параметры (400).
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, anyhow::anyhow!(message.into()))
    }

    /// Ресурс не найден (404).
    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, anyhow::anyhow!(message.into()))
    }

    /// Конфликт состояния (409): например, правка источника,
    /// помеченного к удалению (ADR-0005).
    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self(StatusCode::CONFLICT, anyhow::anyhow!(message.into()))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(json!({
                "detail": format!("{}", self.1)
            })),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    // При преобразовании ошибки по умолчанию присваиваем код серверной ошибки
    fn from(err: E) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, err.into())
    }
}
