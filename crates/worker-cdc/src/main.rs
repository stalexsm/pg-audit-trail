use std::process::ExitCode;

use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use worker_cdc::WorkerConfig;

#[tokio::main]
async fn main() -> ExitCode {
    // Общая обвязка логирования workspace (waltrail-log): фильтр по RUST_LOG,
    // файл с ротацией по дням и вывод в терминал. Guard неблокирующего
    // писателя держим до конца работы процесса.
    let _log_guard = match waltrail_log::init("worker-cdc.log") {
        Ok(guard) => guard,
        // Логирование ещё не поднято: подписчика нет, текст ошибки в журнал
        // не попадёт. Ненулевой код выхода — сигнал оркестратору о фатальной
        // ошибке запуска (тикет #47).
        Err(e) => {
            error!(error = %e, "инициализация логирования worker-cdc");
            return ExitCode::FAILURE;
        }
    };

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
        tx_buffer_max_rows = config.tx_buffer_max_rows,
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
