use std::sync::Arc;

use anyhow::anyhow;
use axum::{
    Json,
    extract::{Request, State},
    http::{self, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use casbin::{CoreApi, Enforcer};
use jsonwebtoken::{DecodingKey, Validation, decode};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::error;
use uuid::Uuid;

pub mod dto;
pub mod handlers;
pub mod repositories;
pub mod routes;

pub type AppResult<T, E = AppError> = core::result::Result<T, E>;

pub struct AppError(StatusCode, anyhow::Error);

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

#[derive(Debug, Serialize, Deserialize)]
struct Role {
    id: Uuid,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    id: i64,
    // login: String,
    // fio: String,
    role: Role,
    // exp: f64,
}

/// Функция для проверки авторизации по токену.
async fn check_authenticate(
    State((jwt_seckey, enforcer)): State<(String, Arc<RwLock<Enforcer>>)>,
    req: Request,
    next: Next,
) -> Result<Response, AppError> {
    // Middleware Authenticate

    let token = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok());

    let token = if let Some(t) = token {
        t.replace("Bearer ", "")
    } else {
        return Err(AppError(
            StatusCode::UNAUTHORIZED,
            anyhow!("Вы не авторизованы!"),
        ));
    };

    match decode::<Claims>(
        &token,
        &DecodingKey::from_secret(jwt_seckey.as_ref()),
        &Validation::default(),
    ) {
        Ok(data) => {
            let path = req.uri().path();
            let method = req.method().as_str();
            match enforcer
                .read()
                .enforce((&data.claims.role.id, path, method))
            {
                Ok(authorized) => {
                    if !authorized {
                        return Err(AppError(
                            StatusCode::FORBIDDEN,
                            anyhow!("Недостаточно прав для доступа!"),
                        ));
                    }
                }
                Err(e) => {
                    error!(error=%e, "Ошибка обработки политики доступов!");
                    return Err(AppError(
                        StatusCode::BAD_GATEWAY,
                        anyhow!("Ошибка обработки политики доступов!"),
                    ));
                }
            }
        }
        Err(e) => {
            error!(error=%e, "Ошибка обработки токена!");
            return Err(AppError(
                StatusCode::UNAUTHORIZED,
                anyhow!("Вы не авторизованы!"),
            ));
        }
    }

    Ok(next.run(req).await)
}
