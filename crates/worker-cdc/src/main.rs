use anyhow::Context;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use worker_cdc::WorkerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Общая обвязка логирования workspace (waltrail-log): фильтр по RUST_LOG,
    // файл с ротацией по дням и вывод в терминал. Guard неблокирующего
    // писателя держим до конца работы процесса.
    let _log_guard = waltrail_log::init("worker-cdc.log").context("инициализация логирования")?;

    let config = WorkerConfig::from_env().context("переменные окружения воркера CDC")?;
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

    if let Err(e) = worker_cdc::run(config, cancellation_token).await {
        error!(error = %e, "Ошибка супервизора worker-cdc!");
    }

    info!("worker-cdc завершён");

    Ok(())
}
