use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_appender::rolling;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worker_debezium::debezium::{self, ProcessorConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let log_file = rolling::daily("logs", "worker-debezium.log"); // Лог-файл, создается каждый день
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

    // Получение env.KAFKA_BROKERS
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| {
        panic!("An error occurred while getting `KAFKA_BROKERS` from ENV. Add a variable to the environment!")
    });

    // Получение env.KAFKA_GROUP_ID
    let group = std::env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| {
        panic!("An error occurred while getting `KAFKA_GROUP_ID` from ENV. Add a variable to the environment!")
    });

    // Получение env.KAFKA_TOPICS
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| {
        panic!("An error occurred while getting `KAFKA_TOPIC` from ENV. Add a variable to the environment!")
    });

    // Получение env.DATABASE_URL
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        panic!("An error occurred while getting `DATABASE_URL` from ENV. Add a variable to the environment!")
    });

    // Создание конфигурации с возможностью переопределения из ENV
    let config = ProcessorConfig {
        batch_size: std::env::var("BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000),
        concurrent_processors: std::env::var("CONCURRENT_PROCESSORS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10),
        chunk_size: std::env::var("CHUNK_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500),
        channel_buffer_size: std::env::var("CHANNEL_BUFFER_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000),
        batch_timeout_ms: std::env::var("BATCH_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100),
        max_retries: std::env::var("MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3),
    };

    info!(
        batch_size = config.batch_size,
        concurrent_processors = config.concurrent_processors,
        chunk_size = config.chunk_size,
        "Конфигурация Debezium воркера"
    );

    // Подключение к БД
    let pool = PgPoolOptions::new()
        .min_connections(3) // Поддерживать минимальное количество соединений
        .max_connections(15)
        .max_lifetime(Duration::from_secs(1800)) // 30 минут
        .idle_timeout(Duration::from_secs(600)) // 10 минут
        .acquire_timeout(Duration::from_secs(30))
        .connect(&url)
        .await
        .unwrap_or_else(|_| panic!("Failed to create Postgres connection pool! URL: {url}"));

    // Запуск миграций
    sqlx::migrate!("../../migrations").run(&pool).await?;

    // Создание токена для graceful shutdown
    let cancellation_token = CancellationToken::new();

    // Настройка обработки сигналов для graceful shutdown
    let shutdown_token = cancellation_token.clone();
    tokio::spawn(async move {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to register SIGTERM handler");
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("Failed to register SIGINT handler");

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

    // Запуск циклов обработки сообщений!
    info!("Starting worker-debezium service...");

    if let Err(e) = debezium::run_loop(
        pool,
        brokers,
        group,
        topic,
        Some(config),
        cancellation_token,
    )
    .await
    {
        error!(error=%e,"Error in debezium task!");
    }

    info!("Worker-debezium service завершен");

    Ok(())
}
