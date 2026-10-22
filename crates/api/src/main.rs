use anyhow::Context;
use api::routes::{ApiDoc, api_router};
use api::startup::{connect_pool, database_url_from_env, port_from_env};
use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::signal;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use ::api::AppResult;

/// Хост, на котором слушает HTTP-сервер (в контейнере — все интерфейсы).
const BIND_HOST: &str = "0.0.0.0";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Общая обвязка логирования workspace (waltrail-log): фильтр по RUST_LOG,
    // файл с ротацией по дням и вывод в терминал. Guard неблокирующего
    // писателя держим до конца работы процесса.
    let _log_guard = waltrail_log::init("api.log").context("инициализация логирования")?;

    // Строка подключения не включается в текст ошибки: пароль из DSN не
    // должен уходить в логи контейнера при неудачном старте.
    let pool = connect_pool(&database_url_from_env()?).await?;

    let port = port_from_env()?;
    let listener = TcpListener::bind((BIND_HOST, port))
        .await
        .with_context(|| format!("не удалось занять порт {BIND_HOST}:{port}"))?;
    info!("listening on {}", listener.local_addr()?);

    // Запуск миграций
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .context("не удалось применить миграции")?;

    axum::serve(
        listener,
        Router::new()
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .route("/health", get(health))
            .merge(api_router())
            .fallback(handler_404)
            .layer(TraceLayer::new_for_http())
            .layer(CorsLayer::new().allow_origin(Any).allow_headers(Any))
            .layer(CompressionLayer::new())
            .with_state(pool)
            .into_make_service(),
    )
    // В контейнере процесс — PID 1 без init: без установленного обработчика
    // ядро не доставит SIGTERM, и `docker stop` ждал бы таймаута до SIGKILL.
    .with_graceful_shutdown(shutdown_signal())
    .await
    .context("работа HTTP-сервера")?;

    Ok(())
}

/// Остановка api по SIGTERM/SIGINT: сервер перестаёт принимать новые
/// соединения и дождивается активных запросов (конвенция остановки по
/// сигналам — как у worker-cdc). Ошибка регистрации обработчиков не
/// паникует: логируем и продолжаем, процесс остановит SIGKILL оркестратора.
async fn shutdown_signal() {
    if let Err(err) = shutdown_signal_result().await {
        error!(error = %err, "Не удалось зарегистрировать обработчики сигналов остановки");
    }
}

async fn shutdown_signal_result() -> anyhow::Result<()> {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .context("Не удалось зарегистрировать обработчик SIGTERM")?;
    let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
        .context("Не удалось зарегистрировать обработчик SIGINT")?;

    tokio::select! {
        _ = sigterm.recv() => info!("Получен SIGTERM, останавливаем api"),
        _ = sigint.recv() => info!("Получен SIGINT (Ctrl+C), останавливаем api"),
    }

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
