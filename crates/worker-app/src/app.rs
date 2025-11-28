use futures::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::OwnedMessage,
};
use redis::{AsyncCommands, aio::MultiplexedConnection};
use serde::Deserialize;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    Result,
    database::{AuditDTO, insert_rows_audit},
};

/// Конфигурация для обработчика App
#[derive(Debug, Clone)]
pub struct AppProcessorConfig {
    pub batch_size: usize,
    pub max_poll_timeout_ms: u64,
    pub concurrent_processors: usize,
    pub channel_buffer_size: usize,
    pub max_retries: usize,
    pub redis_timeout_ms: u64,
    pub kafka_session_timeout_ms: u64,
}

impl Default for AppProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_poll_timeout_ms: 500,
            concurrent_processors: 5,
            channel_buffer_size: 1000,
            max_retries: 3,
            redis_timeout_ms: 5000,
            kafka_session_timeout_ms: 6000,
        }
    }
}

/// Метрики для мониторинга App processor
#[derive(Debug, Default)]
pub struct AppProcessorMetrics {
    pub processed_messages: std::sync::atomic::AtomicU64,
    pub failed_messages: std::sync::atomic::AtomicU64,
    pub redis_errors: std::sync::atomic::AtomicU64,
    pub batches_processed: std::sync::atomic::AtomicU64,
    pub empty_payloads: std::sync::atomic::AtomicU64,
}

impl AppProcessorMetrics {
    pub fn increment_processed(&self) {
        self.processed_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_failed(&self) {
        self.failed_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_redis_errors(&self) {
        self.redis_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_batches(&self) {
        self.batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_empty_payloads(&self) {
        self.empty_payloads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Debug, Deserialize)]
pub struct AppData {
    pub prefix: String,
    pub status_code: i16,
    pub event: String,
    pub primary_key: Option<String>,
    pub response_body: String,
    pub context: serde_json::Value,
}

/// Структура для внутренней передачи сообщений
#[derive(Debug)]
struct MessageBatch {
    messages: Vec<OwnedMessage>,
}

/// Главная функция обработки с поддержкой graceful shutdown
pub async fn run_loop(
    pool: PgPool,
    redis: redis::Client,
    brokers: String,
    group: String,
    topic: String,
    config: Option<AppProcessorConfig>,
    cancellation: CancellationToken,
) -> Result<()> {
    let config = config.unwrap_or_default();
    let metrics = Arc::new(AppProcessorMetrics::default());
    let group = format!("{group}_app");

    info!(
        topic = topic,
        group = group,
        batch_size = config.batch_size,
        concurrent_processors = config.concurrent_processors,
        "Запуск обработки App с улучшенной конфигурацией"
    );

    let consumer = create_kafka_consumer(&brokers, &group, &config)?;
    let consumer = Arc::new(consumer);

    // Подписка на темы
    consumer.subscribe(&[&topic])?;

    // Создаем пул Redis соединений
    let redis_pool = Arc::new(redis);

    // Канал для передачи батчей
    let (tx, rx) = mpsc::channel::<MessageBatch>(config.channel_buffer_size);

    // Спавним задачу для сбора сообщений
    let consumer_clone = Arc::clone(&consumer);
    let metrics_clone = Arc::clone(&metrics);
    let config_clone = config.clone();
    let cancellation_clone = cancellation.clone();

    let collector_task = tokio::spawn(async move {
        message_collector(
            consumer_clone,
            tx,
            config_clone,
            metrics_clone,
            cancellation_clone,
        )
        .await;
    });

    // Обработка батчей
    let processor_task = tokio::spawn({
        let pool = pool.clone();
        let redis_pool = Arc::clone(&redis_pool);
        let consumer = Arc::clone(&consumer);
        let metrics = Arc::clone(&metrics);
        let config = config.clone();
        let cancellation = cancellation.clone();

        async move {
            process_batches(
                pool,
                redis_pool,
                consumer,
                rx,
                config,
                metrics,
                cancellation,
            )
            .await;
        }
    });

    // Ждем завершения или сигнала остановки
    tokio::select! {
        _ = cancellation.cancelled() => {
            info!("Получен сигнал остановки, завершаем обработку");
        }
        _ = collector_task => {
            info!("Message collector завершен");
        }
        _ = processor_task => {
            info!("Batch processor завершен");
        }
    }

    // Логируем финальные метрики
    info!(
        processed_messages = metrics
            .processed_messages
            .load(std::sync::atomic::Ordering::Relaxed),
        failed_messages = metrics
            .failed_messages
            .load(std::sync::atomic::Ordering::Relaxed),
        redis_errors = metrics
            .redis_errors
            .load(std::sync::atomic::Ordering::Relaxed),
        batches_processed = metrics
            .batches_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        empty_payloads = metrics
            .empty_payloads
            .load(std::sync::atomic::Ordering::Relaxed),
        "Статистика обработки App"
    );

    Ok(())
}

/// Создание Kafka Consumer
fn create_kafka_consumer(
    brokers: &str,
    group: &str,
    config: &AppProcessorConfig,
) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set(
            "session.timeout.ms",
            config.kafka_session_timeout_ms.to_string(),
        )
        .set("heartbeat.interval.ms", "2000")
        .set("max.poll.interval.ms", "300000")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.max.bytes", "52428800")
        .set("max.partition.fetch.bytes", "1048576")
        // Настройки для восстановления соединения
        .set("reconnect.backoff.ms", "1000") // Начальная задержка 1 сек
        .set("reconnect.backoff.max.ms", "10000") // Максимальная задержка 10 сек
        .set("retry.backoff.ms", "100") // Задержка между retry 100мс // Таймаут запроса 30 сек
        .set("metadata.request.timeout.ms", "60000") // Таймаут метаданных 60 сек
        .create()?;

    Ok(consumer)
}

/// Коллектор сообщений из Kafka
async fn message_collector(
    consumer: Arc<StreamConsumer>,
    sender: mpsc::Sender<MessageBatch>,
    config: AppProcessorConfig,
    metrics: Arc<AppProcessorMetrics>,
    cancellation: CancellationToken,
) {
    let mut consumer_msg_stream = consumer.stream();
    let mut messages = Vec::with_capacity(config.batch_size);

    loop {
        if cancellation.is_cancelled() {
            info!("Получен сигнал остановки message collector");
            break;
        }

        let timeout = tokio::time::sleep(Duration::from_millis(config.max_poll_timeout_ms));
        tokio::pin!(timeout);

        // Собираем сообщения до таймаута или достижения размера батча
        loop {
            tokio::select! {
                _ = &mut timeout => break,
                borrowed_message = consumer_msg_stream.next() => {
                    match borrowed_message {
                        Some(Ok(msg)) => {
                            // Преобразуем BorrowedMessage в OwnedMessage
                            let owned_msg = msg.detach();
                            messages.push(owned_msg);

                            if messages.len() >= config.batch_size {
                                break;
                            }
                        }
                        Some(Err(err)) => {
                            error!(error = %err, "Ошибка получения сообщения из Kafka");
                            metrics.increment_failed();
                        }
                        None => {
                            warn!("Kafka stream завершен");
                            return;
                        }
                    }
                }
                _ = cancellation.cancelled() => {
                    info!("Получен сигнал остановки во время сбора сообщений");
                    return;
                }
            }
        }

        if !messages.is_empty() {
            let batch = MessageBatch {
                messages: std::mem::take(&mut messages),
            };

            if let Err(e) = sender.send(batch).await {
                error!(error = %e, "Ошибка отправки батча для обработки");
                break;
            }

            messages = Vec::with_capacity(config.batch_size);
        }
    }

    // Отправляем оставшиеся сообщения
    if !messages.is_empty() {
        let batch = MessageBatch { messages };
        if let Err(e) = sender.send(batch).await {
            error!(error = %e, "Ошибка отправки финального батча");
        }
    }
}

/// Обработчик батчей
async fn process_batches(
    pool: PgPool,
    redis_pool: Arc<redis::Client>,
    consumer: Arc<StreamConsumer>,
    mut receiver: mpsc::Receiver<MessageBatch>,
    config: AppProcessorConfig,
    metrics: Arc<AppProcessorMetrics>,
    cancellation: CancellationToken,
) {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.concurrent_processors));

    while let Some(batch) = receiver.recv().await {
        if cancellation.is_cancelled() {
            info!("Получен сигнал остановки batch processor");
            break;
        }

        let pool = pool.clone();
        let redis_pool = Arc::clone(&redis_pool);
        let consumer = Arc::clone(&consumer);
        let metrics = Arc::clone(&metrics);
        let config = config.clone();
        let semaphore = Arc::clone(&semaphore);

        tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            if let Err(e) =
                process_single_batch(pool, redis_pool, consumer, batch, config, metrics).await
            {
                error!(error = %e, "Ошибка обработки батча");
            }
        });
    }
}

/// Обработка одного батча
async fn process_single_batch(
    pool: PgPool,
    redis_pool: Arc<redis::Client>,
    consumer: Arc<StreamConsumer>,
    batch: MessageBatch,
    config: AppProcessorConfig,
    metrics: Arc<AppProcessorMetrics>,
) -> Result<()> {
    if batch.messages.is_empty() {
        return Ok(());
    }

    // Обработка данных с retry логикой
    let mut retry_count = 0;
    let records = loop {
        match owned_message_to_audit_dto(&redis_pool, &batch.messages, &metrics).await {
            Ok(records) => break records,
            Err(e) => {
                retry_count += 1;
                if retry_count >= config.max_retries {
                    error!(error = %e, "Максимальное количество попыток обработки данных исчерпано");
                    return Err(e);
                }

                warn!(
                    error = %e,
                    retry_count,
                    "Ошибка обработки данных, повторная попытка"
                );

                let delay = Duration::from_millis(100 * (1 << retry_count));
                tokio::time::sleep(delay).await;
            }
        }
    };

    info!(message_cnt = records.len(), "Сообщений для обработки");
    metrics.increment_batches();

    // Обработка в БД с retry логикой
    retry_count = 0;
    loop {
        let mut tx = pool.begin().await?;

        match insert_rows_audit(&mut tx, &records).await {
            Ok(_) => {
                if let Err(err) = tx.commit().await {
                    retry_count += 1;
                    if retry_count >= config.max_retries {
                        error!(error = %err, "Максимальное количество попыток коммита исчерпано");
                        return Err(err.into());
                    }

                    warn!(
                        error = %err,
                        retry_count,
                        "Ошибка коммита транзакции, повторная попытка"
                    );

                    let delay = Duration::from_millis(100 * (1 << retry_count));
                    tokio::time::sleep(delay).await;
                    continue;
                }

                // Используем commit_consumer_state вместо commit_message
                if let Err(err) = consumer.commit_consumer_state(CommitMode::Async) {
                    warn!(error = %err, "Ошибка коммита offset в Kafka");
                    // Не прерываем обработку, т.к. данные уже сохранены
                }

                metrics.increment_processed();
                break;
            }
            Err(err) => {
                let _ = tx.rollback().await;
                retry_count += 1;

                if retry_count >= config.max_retries {
                    error!(error = %err, "Максимальное количество попыток вставки исчерпано");
                    return Err(err);
                }

                warn!(
                    error = %err,
                    retry_count,
                    "Ошибка вставки данных в БД, повторная попытка"
                );

                let delay = Duration::from_millis(100 * (1 << retry_count));
                tokio::time::sleep(delay).await;
            }
        }
    }

    Ok(())
}

/// Переименованная функция для работы с OwnedMessage
async fn owned_message_to_audit_dto(
    redis_pool: &redis::Client,
    messages: &[OwnedMessage],
    metrics: &AppProcessorMetrics,
) -> Result<Vec<AuditDTO>> {
    let mut con = redis_pool.get_multiplexed_async_connection().await?;
    let mut audit_dto_vec = Vec::with_capacity(messages.len());

    for message in messages {
        let payload = match message.payload() {
            Some(data) => data,
            None => {
                warn!("Пустое тело payload");
                metrics.increment_empty_payloads();
                continue;
            }
        };

        let app_data = match serde_json::from_slice::<AppData>(payload) {
            Ok(data) => data,
            Err(e) => {
                error!(error = %e, "Ошибка декодирования сообщения JSON");
                metrics.increment_failed();
                continue;
            }
        };

        let response_body = if !app_data.response_body.is_empty() {
            match get_response_body_with_timeout(&mut con, &app_data.response_body).await {
                Ok(body) => body,
                Err(e) => {
                    error!(error = %e, "Ошибка получения body из Redis");
                    metrics.increment_redis_errors();
                    String::new()
                }
            }
        } else {
            String::new()
        };

        let id = extract_request_id(&app_data.context);

        let audit_dto = AuditDTO::new(
            id,
            app_data.event,
            app_data.primary_key,
            app_data.status_code,
            response_body,
            app_data.context,
        );

        audit_dto_vec.push(audit_dto);
    }

    Ok(audit_dto_vec)
}

/// Извлечение request_id из контекста с fallback на новый UUID
fn extract_request_id(context: &serde_json::Value) -> uuid::Uuid {
    context
        .get("request_id")
        .and_then(|request_id| request_id.as_str())
        .and_then(|str_id| uuid::Uuid::parse_str(str_id).ok())
        .unwrap_or_else(uuid::Uuid::new_v4)
}

/// Получение response body из Redis с таймаутом
async fn get_response_body_with_timeout(
    con: &mut MultiplexedConnection,
    key: &str,
) -> Result<String> {
    let timeout = Duration::from_millis(5000); // 5 секунд таймаут

    let result = tokio::time::timeout(timeout, con.get_del(key)).await;

    match result {
        Ok(Ok(body)) => Ok(body),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(anyhow::anyhow!("Таймаут при получении данных из Redis")),
    }
}
