use anyhow::anyhow;
use base64::Engine;
use chrono::{DateTime, Utc};
use fancy_regex::Regex;
use futures::StreamExt;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
};
use serde::Deserialize;
use sqlx::PgPool;
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    Result,
    database::{ChangeDTO, insert_rows_change},
};

/// Конфигурация для обработчика Debezium
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    pub batch_size: usize,
    pub concurrent_processors: usize,
    pub chunk_size: usize,
    pub channel_buffer_size: usize,
    pub batch_timeout_ms: u64,
    pub max_retries: usize,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 10000,
            concurrent_processors: 10,
            chunk_size: 500,
            channel_buffer_size: 1000,
            batch_timeout_ms: 100,
            max_retries: 3,
        }
    }
}

/// Метрики для мониторинга
#[derive(Debug, Default)]
pub struct ProcessorMetrics {
    pub processed_messages: std::sync::atomic::AtomicU64,
    pub failed_messages: std::sync::atomic::AtomicU64,
    pub batches_processed: std::sync::atomic::AtomicU64,
    pub processing_errors: std::sync::atomic::AtomicU64,
}

impl ProcessorMetrics {
    pub fn increment_processed(&self) {
        self.processed_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_failed(&self) {
        self.failed_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_batches(&self) {
        self.batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_errors(&self) {
        self.processing_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

const PATTERNS: [&str; 3] = [
    r"(?i)^\s*INSERT\s+INTO\s+([^\s\(]+)",
    r"(?i)^\s*UPDATE\s+([^\s]+)",
    r"(?i)^\s*DELETE\s+FROM\s+([^\s]+)",
];

// Используем современный OnceLock вместо lazy_static
static COMPILED_PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
static EMPTY_JSON: OnceLock<serde_json::Value> = OnceLock::new();

fn get_compiled_patterns() -> &'static Vec<Regex> {
    COMPILED_PATTERNS.get_or_init(|| PATTERNS.iter().filter_map(|p| Regex::new(p).ok()).collect())
}

fn empty_json() -> &'static serde_json::Value {
    EMPTY_JSON.get_or_init(|| serde_json::json!({}))
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
enum Operation {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "t")]
    Truncate,
    #[serde(rename = "m")]
    Message,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Create => write!(f, "CREATE"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
            Self::Read => write!(f, "READ"),
            Self::Truncate => write!(f, "TRUNCATE"),
            Self::Message => write!(f, "MESSAGE"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct DebeziumData {
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub source: DebeziumSource,
    #[serde(rename = "op")]
    pub operation: Operation,
    pub ts_ms: i64,
    pub message: Option<DebeziumMessage>,
}

#[derive(Debug, Deserialize)]
struct DebeziumMessage {
    pub prefix: String,
    pub content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DebeziumSource {
    pub db: String,
    pub table: String,
    pub schema: String,
    #[serde(rename = "txId")]
    pub tx_id: i64,
    pub lsn: i64,
    pub ts_ms: i64,
}

const PREFIX_AUDIT_CONTEXT: &str = "_audit";

/// Главная функция обработки с поддержкой graceful shutdown и конфигурации
pub async fn run_loop(
    pool: PgPool,
    brokers: String,
    group: String,
    topic: String,
    config: Option<ProcessorConfig>,
    cancellation: CancellationToken,
) -> Result<()> {
    let config = config.unwrap_or_default();
    let metrics = Arc::new(ProcessorMetrics::default());

    info!(
        topic = topic,
        group = group,
        batch_size = config.batch_size,
        concurrent_processors = config.concurrent_processors,
        "Запуск обработки Debezium"
    );

    let consumer = match create_kafka_consumer(&brokers, &group) {
        Ok(consumer) => Arc::new(consumer),
        Err(err) => {
            error!(error = %err, "Не удалось создать Kafka-потребителя для обработки CDC");
            return Err(anyhow!(err));
        }
    };

    // Подписываемся на топик
    if let Err(err) = consumer.subscribe(&[&topic]) {
        error!(error = %err, "Не удалось подписаться на Kafka-топики для обработки CDC");
        return Err(anyhow!(err));
    }

    // Канал для передачи собранных батчей с настраиваемым буфером
    let (tx, rx) = mpsc::channel::<Vec<ChangeDTO>>(config.channel_buffer_size);

    // Спавним задачу для сборки батчей
    let consumer_clone = Arc::clone(&consumer);
    let tx_clone = tx.clone();
    let config_clone = config.clone();
    let metrics_clone = Arc::clone(&metrics);
    let cancellation_clone = cancellation.clone();

    let kafka_task = tokio::spawn(async move {
        kafka_batch_consumer(
            consumer_clone,
            tx_clone,
            config_clone,
            metrics_clone,
            cancellation_clone,
        )
        .await;
    });

    // Обработка батчей с контролем ресурсов
    let rx_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let consumer_for_commit = Arc::clone(&consumer);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.concurrent_processors));

    let processing_task = tokio::spawn({
        let pool = pool.clone();
        let config = config.clone();
        let metrics = Arc::clone(&metrics);
        let cancellation = cancellation.clone();

        async move {
            rx_stream
                .for_each_concurrent(None, |batch| {
                    let pool = pool.clone();
                    let consumer = Arc::clone(&consumer_for_commit);
                    let config = config.clone();
                    let metrics = Arc::clone(&metrics);
                    let semaphore = Arc::clone(&semaphore);
                    let cancellation = cancellation.clone();

                    async move {
                        // Проверяем cancellation перед обработкой
                        if cancellation.is_cancelled() {
                            return;
                        }

                        // Получаем разрешение от семафора
                        let _permit = match semaphore.acquire().await {
                            Ok(permit) => permit,
                            Err(_) => {
                                warn!("Семафор закрыт, пропускаем батч");
                                return;
                            }
                        };

                        if batch.is_empty() {
                            return;
                        }

                        let batch_size = batch.len();
                        info!(batch_size, "Обработка пакета данных");

                        // Обрабатываем данные
                        let processed_data = process_batch_data(batch);
                        metrics.increment_batches();

                        // Обрабатываем по чанкам с retry логикой
                        for chunk in processed_data.chunks(config.chunk_size) {
                            let mut retry_count = 0;

                            while retry_count < config.max_retries {
                                if cancellation.is_cancelled() {
                                    return;
                                }

                                match store_chunk_with_transaction(&pool, chunk, &consumer).await {
                                    Ok(_) => {
                                        metrics.increment_processed();
                                        break;
                                    }
                                    Err(e) => {
                                        retry_count += 1;
                                        error!(
                                            error = %e,
                                            retry_count,
                                            max_retries = config.max_retries,
                                            "Ошибка при сохранении чанка, повторная попытка"
                                        );

                                        if retry_count >= config.max_retries {
                                            metrics.increment_errors();
                                            error!("Максимальное количество попыток исчерпано для чанка");
                                            // Здесь можно добавить отправку в DLQ
                                        } else {
                                            // Экспоненциальная задержка
                                            let delay = Duration::from_millis(100 * (1 << retry_count));
                                            tokio::time::sleep(delay).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .await;
        }
    });

    // Ждем завершения или сигнала остановки
    tokio::select! {
        _ = cancellation.cancelled() => {
            info!("Получен сигнал остановки, завершаем обработку");
        }
        _ = kafka_task => {
            info!("Kafka consumer завершен");
        }
        _ = processing_task => {
            info!("Обработчик батчей завершен");
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
        batches_processed = metrics
            .batches_processed
            .load(std::sync::atomic::Ordering::Relaxed),
        processing_errors = metrics
            .processing_errors
            .load(std::sync::atomic::Ordering::Relaxed),
        "Статистика обработки"
    );

    Ok(())
}

/// Обрабатывает пакет данных с улучшенной обработкой ошибок
fn process_batch_data(batch: Vec<ChangeDTO>) -> Vec<ChangeDTO> {
    // Сортируем по position
    let mut changes = batch;
    changes.sort_by(|a, b| a.position.cmp(&b.position));

    // Агрегируем контекст для операций MESSAGE
    let context_map: HashMap<(i64, Option<String>), serde_json::Value> = changes
        .par_iter()
        .filter(|change| change.operation == Operation::Message.to_string())
        .filter_map(|change| {
            extract_table_from_sql(&change.context)
                .map(|table| ((change.transaction_id, Some(table)), change.context.clone()))
        })
        .collect();

    info!(count = context_map.len(), "Построена карта контекстов");

    // Применяем найденный контекст к остальным сообщениям
    changes
        .into_par_iter()
        .filter(|change| change.operation != Operation::Message.to_string())
        .map(|mut change| {
            if let Some(ctx) = context_map.get(&(change.transaction_id, Some(change.table.clone())))
            {
                change.context = ctx.clone();
                change.request_id = extract_request_id(ctx);
            }
            change
        })
        .collect()
}

/// Извлекает имя таблицы из SQL с улучшенной обработкой ошибок
fn extract_table_from_sql(context: &serde_json::Value) -> Option<String> {
    let sql = context.get("SQL")?.as_str()?;

    let patterns = get_compiled_patterns();

    for regex in patterns.iter() {
        match regex.is_match(sql) {
            Ok(true) => match regex.captures(sql) {
                Ok(Some(captures)) => {
                    if let Some(table_match) = captures.get(1) {
                        return Some(table_match.as_str().trim().to_string());
                    }
                }
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, "Ошибка при извлечении captures из regex");
                    continue;
                }
            },
            Ok(false) => continue,
            Err(e) => {
                warn!(error = %e, "Ошибка при проверке regex");
                continue;
            }
        }
    }

    None
}

/// Извлекает request_id из контекста
fn extract_request_id(context: &serde_json::Value) -> Option<String> {
    context.get("request_id")?.as_str().map(|s| s.to_string())
}

/// Сохраняет чанк данных с улучшенной обработкой ошибок
async fn store_chunk_with_transaction(
    pool: &PgPool,
    chunk: &[ChangeDTO],
    consumer: &Arc<StreamConsumer>,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    match insert_rows_change(&mut tx, chunk).await {
        Ok(_) => {
            tx.commit().await?;

            // Асинхронный commit offset'ов с обработкой ошибок
            if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                // Логируем ошибку, но не прерываем обработку
                warn!(error = %e, "Не удалось закоммитить offset, будет повторная обработка");
            }

            Ok(())
        }
        Err(e) => {
            if let Err(rollback_err) = tx.rollback().await {
                error!(error = %rollback_err, "Ошибка при откате транзакции");
            }
            Err(anyhow!("Ошибка сохранения данных: {}", e))
        }
    }
}

/// Улучшенная функция для сборки батчей с graceful shutdown
async fn kafka_batch_consumer(
    consumer: Arc<StreamConsumer>,
    sender: mpsc::Sender<Vec<ChangeDTO>>,
    config: ProcessorConfig,
    metrics: Arc<ProcessorMetrics>,
    cancellation: CancellationToken,
) {
    let mut batch = Vec::with_capacity(config.batch_size);
    let mut consumer_stream = consumer.stream();

    // Семафор для контроля количества обрабатываемых батчей
    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.concurrent_processors * 2,
    ));

    loop {
        if cancellation.is_cancelled() {
            info!("Получен сигнал остановки consumer'а");
            break;
        }

        tokio::select! {
            maybe_message = consumer_stream.next() => {
                match maybe_message {
                    Some(Ok(message)) => {
                        if let Some(payload) = message.payload() {
                            match payload_to_change_dto(payload) {
                                Ok(change) => {
                                    batch.push(change);
                                    metrics.increment_processed();
                                }
                                Err(e) => {
                                    error!(error = %e, "Ошибка при обработке сообщения");
                                    metrics.increment_failed();
                                }
                            }
                        }

                        // Отправляем батч если он заполнен
                        if batch.len() >= config.batch_size {
                            send_batch(&mut batch, &sender, &semaphore, &config).await;
                        }
                    }
                    Some(Err(e)) => {
                        error!(error = %e, "Ошибка при получении сообщения от Kafka");
                        metrics.increment_failed();
                    }
                    None => {
                        warn!("Kafka stream завершен");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(config.batch_timeout_ms)) => {
                // Отправляем неполный батч по таймауту
                if !batch.is_empty() {
                    send_batch(&mut batch, &sender, &semaphore, &config).await;
                }
            }
            _ = cancellation.cancelled() => {
                info!("Получен сигнал остановки, завершаем consumer");
                break;
            }
        }
    }

    // Отправляем оставшиеся сообщения
    if !batch.is_empty() {
        send_batch(&mut batch, &sender, &semaphore, &config).await;
    }
}

/// Отправляет батч с контролем backpressure
async fn send_batch(
    batch: &mut Vec<ChangeDTO>,
    sender: &mpsc::Sender<Vec<ChangeDTO>>,
    semaphore: &Arc<tokio::sync::Semaphore>,
    config: &ProcessorConfig,
) {
    let batch_to_send = std::mem::replace(batch, Vec::with_capacity(config.batch_size));

    // Пытаемся получить разрешение от семафора
    if let Ok(permit) = semaphore.clone().try_acquire_owned() {
        let sender_clone = sender.clone();

        tokio::spawn(async move {
            if let Err(e) = sender_clone.send(batch_to_send).await {
                error!(error = %e, "Ошибка при отправке батча");
            }
            // Разрешение освобождается автоматически
            drop(permit);
        });
    } else {
        // Если семафор заполнен, отправляем синхронно
        if let Err(e) = sender.send(batch_to_send).await {
            error!(error = %e, "Ошибка при отправке батча (fallback)");
        }
    }
}

/// Создание Kafka Consumer с улучшенной конфигурацией
fn create_kafka_consumer(brokers: &str, group: &str) -> Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "30000")
        .set("heartbeat.interval.ms", "3000")
        .set("max.poll.interval.ms", "600000")
        .set("fetch.min.bytes", "1")
        .set("fetch.max.bytes", "52428800") // 50MB
        .set("max.partition.fetch.bytes", "1048576") // 1MB
        .set("auto.offset.reset", "earliest")
        .set("queued.min.messages", "100000") // Буферизация сообщений
        .set("queued.max.messages.kbytes", "1048576") // 1GB буфер
        .set("fetch.wait.max.ms", "500") // Максимальное время ожидания
        // Настройки для восстановления соединения
        .set("reconnect.backoff.ms", "500") // Начальная задержка 0.5 сек
        .set("reconnect.backoff.max.ms", "5000") // Максимальная задержка 5 сек
        .set("retry.backoff.ms", "50") // Задержка между retry 50 мс
        .set("request.timeout.ms", "60000") // Таймаут запроса 60 сек
        .set("metadata.request.timeout.ms", "120000") // Таймаут метаданных 120 сек
        // Дополнительные настройки для стабильности
        .set("socket.keepalive.enable", "true") // Включить keepalive
        .set("socket.timeout.ms", "60000") // Таймаут сокета
        .set("connections.max.idle.ms", "540000") // 9 минут до закрытия idle соединения
        .create::<StreamConsumer>()
        .map_err(|err| anyhow!("KafkaError: {}", err))
}

/// Улучшенное преобразование payload с оптимизированной работой с JSON
fn payload_to_change_dto(payload: &[u8]) -> Result<ChangeDTO> {
    let data: DebeziumData = serde_json::from_slice(payload)?;

    // Извлекаем primary_key для операций UPDATE и DELETE
    let primary_key = extract_primary_key(&data);

    let committed_at = DateTime::<Utc>::from_timestamp_millis(data.source.ts_ms)
        .ok_or_else(|| anyhow!("Не удалось получить timestamp для committed_at"))?;

    let queued_at = DateTime::<Utc>::from_timestamp_millis(data.ts_ms)
        .ok_or_else(|| anyhow!("Не удалось получить timestamp для queued_at"))?;

    // Используем ссылки на статические значения для эффективности
    let before = data.before.as_ref().unwrap_or_else(|| empty_json());
    let after = data.after.as_ref().unwrap_or_else(|| empty_json());

    // Обработка контекста с улучшенной обработкой ошибок
    let context = process_message_context(&data.message)?;

    // Извлекаем request_id из контекста
    let request_id = extract_request_id(&context);

    Ok(ChangeDTO::new(
        data.source.db,
        data.source.schema,
        data.source.table,
        primary_key,
        data.operation.to_string(),
        before.clone(),
        after.clone(),
        context,
        request_id,
        committed_at,
        queued_at,
        Utc::now(),
        data.source.tx_id,
        data.source.lsn,
    ))
}

/// Извлекает primary key с улучшенной обработкой
fn extract_primary_key(data: &DebeziumData) -> Option<String> {
    if matches!(data.operation, Operation::Update | Operation::Delete) {
        data.before.as_ref().and_then(|before| {
            before.as_object()?.get("id").map(|id| match id {
                serde_json::Value::String(s) => s.clone(),
                _ => id.to_string(),
            })
        })
    } else {
        None
    }
}

/// Обрабатывает контекст сообщения с улучшенной обработкой ошибок
fn process_message_context(message: &Option<DebeziumMessage>) -> Result<serde_json::Value> {
    let message = match message {
        Some(m) => m,
        None => return Ok(empty_json().clone()),
    };

    if message.prefix != PREFIX_AUDIT_CONTEXT {
        return Ok(empty_json().clone());
    }

    let encoded_content = match &message.content {
        Some(content) => content,
        None => return Ok(empty_json().clone()),
    };

    // Декодируем base64 с обработкой ошибок
    let decoded_bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded_content)
        .map_err(|e| anyhow!("Ошибка декодирования base64: {}", e))?;

    // Парсим JSON с обработкой ошибок
    let context: serde_json::Value = serde_json::from_slice(&decoded_bytes)
        .map_err(|e| anyhow!("Ошибка парсинга JSON из контекста: {}", e))?;

    Ok(context)
}
