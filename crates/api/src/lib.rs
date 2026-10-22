use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use uuid::Uuid;

pub mod dto;
pub mod handlers;
pub mod repositories;
pub mod routes;
pub mod startup;

pub type AppResult<T, E = AppError> = Result<T, E>;

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

/// Нейтральное сообщение 5xx: детали инфраструктуры (тексты sqlx/PostgreSQL,
/// имена внутренних объектов) клиенту не раскрываются — полный текст ошибки
/// остаётся только в логе (тикет #45).
const INTERNAL_ERROR_DETAIL: &str = "Внутренняя ошибка сервера";

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // Серверная ошибка: текст уходит только в лог; клиент получает
        // нейтральное сообщение и correlation id для сопоставления с логом.
        if self.0.is_server_error() {
            let correlation_id = Uuid::new_v4();
            let err = self.1;
            tracing::error!(
                correlation_id = %correlation_id,
                "Внутренняя ошибка сервера: {err:#}"
            );

            return (
                self.0,
                Json(json!({
                    "detail": INTERNAL_ERROR_DETAIL,
                    "correlation_id": correlation_id,
                })),
            )
                .into_response();
        }

        // Осмысленная ошибка клиента (4xx) сохраняет своё сообщение.
        (self.0, Json(json!({ "detail": format!("{}", self.1) }))).into_response()
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    /// Тело ответа ошибки в JSON.
    async fn error_body(err: AppError) -> (StatusCode, serde_json::Value) {
        let response = err.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("тело ответа");
        (
            status,
            serde_json::from_str(&String::from_utf8_lossy(&bytes)).expect("JSON-ответ"),
        )
    }

    #[tokio::test]
    async fn server_error_is_neutral_and_carries_correlation_id() {
        let (status, body) = error_body(AppError(
            StatusCode::INTERNAL_SERVER_ERROR,
            anyhow::anyhow!("error returned from database: relation \"changes\" does not exist"),
        ))
        .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["detail"], "Внутренняя ошибка сервера");
        // Correlation id в ответе разбирается как UUID — по нему ошибку
        // можно найти в логе.
        Uuid::parse_str(body["correlation_id"].as_str().expect("correlation_id"))
            .expect("correlation id — UUID");
        // Тексты БД и имена внутренних объектов в ответ не попадают.
        let raw = body.to_string();
        for leaked in [
            "error returned from database",
            "relation \"",
            "does not exist",
            "changes",
        ] {
            assert!(!raw.contains(leaked), "5xx-ответ раскрывает {leaked}");
        }
    }

    #[tokio::test]
    async fn client_error_keeps_meaningful_message() {
        let (status, body) = error_body(AppError::bad_request("Имя источника уже занято")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["detail"], "Имя источника уже занято");
        assert!(body.get("correlation_id").is_none());
    }
}
