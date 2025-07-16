use std::time::Duration;

use redis::Client;
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info};
use tracing_appender::rolling;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worker_app::app;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let log_file = rolling::daily("logs", "worker-app.log"); // Лог-файл, создается каждый день
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

    let topic_app = std::env::var("KAFKA_TOPIC_APP").unwrap_or_else(|_| {
        panic!("An error occurred while getting `KAFKA_TOPIC_APP` from ENV. Add a variable to the environment!")
    });

    // Получение env.DATABASE_URL
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        panic!("An error occurred while getting `DATABASE_URL` from ENV. Add a variable to the environment!")
    });

    // Получение env.REDIS_URL
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| {
        panic!("An error occurred while getting `REDIS_URL` from ENV. Add a variable to the environment!")
    });

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

    let redis = Client::open(redis_url)?;
    // Настраиваем параметры пула соединений

    // Запуск миграций
    sqlx::migrate!("../../migrations").run(&pool).await?;

    info!("Starting worker-app service...");

    // Запуск обработка сообщений!
    if let Err(e) = app::run_loop(pool, redis, brokers, group, topic_app).await {
        error!(error=%e, "Error in app task!");
    }

    Ok(())
}
