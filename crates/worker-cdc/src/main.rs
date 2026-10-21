use std::{env, path::PathBuf};

use anyhow::Context;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_appender::rolling;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worker_cdc::WorkerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // В режиме разработки логи пишутся в корень проекта, в проде — рядом с бинарником.
    let base_dir = if cfg!(debug_assertions) {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
    } else {
        let exe = env::current_exe()?;
        exe.parent().map(PathBuf::from).unwrap_or(exe)
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

    let config = WorkerConfig::from_env().context("переменные окружения воркера CDC")?;
    info!(
        slot = %config.slot,
        publication = %config.publication,
        ignore_tables = ?config.ignore_tables,
        batch_size = config.batch_size,
        "Конфигурация воркера CDC"
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

    if let Err(e) = worker_cdc::run(config, cancellation_token).await {
        error!(error = %e, "Ошибка в цикле репликации worker-cdc!");
    }

    info!("worker-cdc завершён");

    Ok(())
}
