use std::{env, path::PathBuf, process::ExitCode};

use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_appender::rolling;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worker_cdc::WorkerConfig;

#[tokio::main]
async fn main() -> ExitCode {
    // В режиме разработки логи пишутся в корень проекта, в проде — рядом с бинарником.
    let base_dir = if cfg!(debug_assertions) {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
    } else {
        match env::current_exe() {
            Ok(exe) => exe.parent().map(PathBuf::from).unwrap_or(exe),
            // Путь бинарника не узнали — логи обязаны писаться рядом с ним,
            // а не где попало: фатальная ошибка запуска.
            Err(e) => {
                error!(error = %e, "не удалось определить путь бинарника для каталога логов");
                return ExitCode::FAILURE;
            }
        }
    };

    let log_file = rolling::daily(base_dir.join("logs"), "worker-cdc.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(log_file);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_level(true)
                .with_target(false),
        )
        .with(
            fmt::layer()
                .with_ansi(true)
                .with_level(true)
                .with_target(false),
        )
        .init();

    // Конфигурация читается до супервизора: некорректное окружение — тоже
    // фатальная ошибка запуска, процесс завершается с ненулевым кодом.
    let config = match WorkerConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            error!(error = %e, "некорректные переменные окружения воркера CDC");
            return ExitCode::FAILURE;
        }
    };
    info!(
        reconnect_initial_backoff_ms = config.reconnect_initial_backoff_ms,
        reconnect_max_backoff_ms = config.reconnect_max_backoff_ms,
        "worker-cdc: супервизор источников — настройки источников читаются из реестра (ADR-0005)"
    );

    // Graceful shutdown по SIGTERM/SIGINT.
    let cancellation_token = CancellationToken::new();
    let shutdown_token = cancellation_token.clone();
    tokio::spawn(async move {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Не удалось зарегистрировать обработчик SIGTERM");
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("Не удалось зарегистрировать обработчик SIGINT");

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Получен SIGTERM, инициируем graceful shutdown");
            }
            _ = sigint.recv() => {
                info!("Получен SIGINT (Ctrl+C), инициируем graceful shutdown");
            }
        }

        shutdown_token.cancel();
    });

    info!("Запуск worker-cdc...");

    // Фатальная ошибка супервизора — недоступность БД аудита, провал
    // миграций — ненулевой код выхода: перезапускающая система (оркестратор,
    // compose restart) видит сбой и реагирует, процесс не живёт впустую
    // (тикет #47).
    if let Err(e) = worker_cdc::run(config, cancellation_token).await {
        error!(
            error = %e,
            "фатальная ошибка супервизора worker-cdc — завершаем процесс с ненулевым кодом выхода"
        );
        return ExitCode::FAILURE;
    }

    info!("worker-cdc завершён");
    ExitCode::SUCCESS
}
