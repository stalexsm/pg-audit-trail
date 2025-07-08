use std::{sync::Arc, time::Duration};

use api::routes::{ApiDoc, api_router};
use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use parking_lot::RwLock;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use ::api::AppResult;
use casbin::{self, CoreApi, Enforcer};
use tracing::info;
use tracing_appender::rolling;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let log_file = rolling::daily("logs", "api.log"); // Лог-файл, создается каждый день
    let (non_blocking, _guard) = tracing_appender::non_blocking(log_file);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Настройка логирования
    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_writer(non_blocking) // Пишем в файл
                .with_ansi(false) // Без цветов
                .with_level(true)
                .with_target(false),
        ) // Убираем target
        .with(
            fmt::layer()
                .with_ansi(true) // Цветной вывод в терминал
                .with_level(true)
                .with_target(false),
        )
        .init();

    // Получение env.DATABASE_URL
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        panic!("An error occurred while getting `DATABASE_URL` from ENV. Add a variable to the environment!")
    });

    let jwt_seckey = std::env::var("JWT_SECKEY").unwrap_or_else(|_| {
        panic!("An error occurred while getting `JWT_SECKEY` from ENV. Add a variable to the environment!")
    });

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&url)
        .await
        .unwrap_or_else(|_| panic!("Failed to create Postgres connection pool! URL: {url}"));

    let listener = TcpListener::bind("0.0.0.0:8000").await?;
    info!("listening on {}", listener.local_addr()?);

    // Casbin для обработки политики достуров...
    let enforcer = Arc::new(RwLock::new(
        Enforcer::new("casbin.conf", "policy.csv").await?,
    ));

    axum::serve(
        listener,
        Router::new()
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .route("/health", get(health))
            .nest("/api", api_router(jwt_seckey, enforcer))
            .fallback(handler_404)
            .layer(TraceLayer::new_for_http())
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_headers(Any)
                    .allow_headers(Any),
            )
            .layer(CompressionLayer::new())
            .with_state(pool)
            .into_make_service(),
    )
    .await?;

    Ok(())
}

#[derive(Debug, Serialize)]
pub struct Health {
    pub ok: bool,
}

//Метод для проверки работоспособности API
async fn health() -> AppResult<Json<Health>> {
    Ok(Json(Health { ok: true }))
}

/// Обработка не существующих запросов.
async fn handler_404() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        "Данной страницы не существует. Проверьте правильность адреса...",
    )
}
