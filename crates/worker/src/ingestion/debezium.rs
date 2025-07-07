use anyhow::anyhow;
use base64::Engine;
use chrono::{DateTime, Utc};
use fancy_regex::Regex;
use futures::StreamExt;
use lazy_static::lazy_static;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
};
use serde::Deserialize;
use sqlx::PgPool;
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::{
    Result,
    database::{ChangeDTO, insert_rows_change},
};

const PATTERNS: [&str; 3] = [
    r"(?i)^\s*INSERT\s+INTO\s+([^\s\(]+)",
    r"(?i)^\s*UPDATE\s+([^\s]+)",
    r"(?i)^\s*DELETE\s+FROM\s+([^\s]+)",
];

lazy_static! {
    static ref COMPILED_PATTERNS: Vec<Regex> =
        PATTERNS.iter().filter_map(|p| Regex::new(p).ok()).collect();
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
const BATCH_SIZE: usize = 10000;

// Используем конфигурируемое количество параллельных обработчиков
const CONCURRENT_PROCESSORS: usize = 10;
const CHUNK_SIZE_IN_DATABASE: usize = 500;

pub async fn run_loop(pool: PgPool, brokers: String, group: String, topic: String) -> Result<()> {
    info!(topic = topic, group = group, "Запуск обработки Debezium!");

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

    // Канал для передачи собранных батчей
    let (tx, rx) = mpsc::channel::<Vec<ChangeDTO>>(1000);

    {
        // Спавним задачу для сборки батчей через consumer.stream()
        let consumer_clone = Arc::clone(&consumer);
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            kafka_batch_consumer(consumer_clone, tx_clone).await;
        });
    }

    // Используем ReceiverStream для параллельной обработки без блокирующего Mutex
    // Оборачиваем rx в ReceiverStream для удобной обработки
    let rx_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let consumer_for_commit = Arc::clone(&consumer);

    rx_stream
        .for_each_concurrent(CONCURRENT_PROCESSORS, |batch| {
            let pool = pool.clone();
            let consumer = Arc::clone(&consumer_for_commit);

            async move {
                if batch.is_empty() {
                    return;
                }

                let batch_size = batch.len();
                info!(batch_size, "Обработка пакета данных");

                // Подготавливаем данные для обработки
                let processed_data = process_batch_data(batch);

                // Обрабатываем подготовленные данные по чанкам
                for chunk in processed_data.chunks(CHUNK_SIZE_IN_DATABASE) {
                    if let Err(e) = store_chunk_with_transaction(&pool, chunk, &consumer).await {
                        error!(error = %e, "Не удалось сохранить чанк данных");
                    }
                }
            }
        })
        .await;

    Ok(())
}

/// Обрабатывает пакет данных: сортирует, агрегирует контекст и применяет его к сообщениям
fn process_batch_data(batch: Vec<ChangeDTO>) -> Vec<ChangeDTO> {
    // Сортируем по position
    let mut changes = batch;
    changes.sort_by(|a, b| a.position.cmp(&b.position));

    // Агрегируем контекст для операций MESSAGE
    let context_map: HashMap<(i64, Option<String>), serde_json::Value> = changes
        .par_iter()
        .filter(|change| change.operation == Operation::Message.to_string())
        .map(|change| {
            let tbl = if let Some(sql) = change.context.get("SQL").and_then(|v| v.as_str()) {
                COMPILED_PATTERNS
                    .par_iter() // Параллельный поиск подходящего шаблона
                    .find_first(|re| re.is_match(sql).unwrap_or(false))
                    .and_then(|re| {
                        re.captures(sql)
                            .ok()
                            .flatten()
                            .and_then(|cap| cap.get(1).map(|m| m.as_str().trim().to_string()))
                    })
            } else {
                None
            };

            ((change.transaction_id, tbl), change.context.clone())
        })
        .collect();

    info!(count = context_map.len(), "Context Map!");

    // Применяем найденный контекст к остальным сообщениям и исключаем MESSAGE
    changes
        .into_par_iter()
        .filter(|change| change.operation != Operation::Message.to_string())
        .map(|mut change| {
            if let Some(ctx) = context_map.get(&(change.transaction_id, Some(change.table.clone())))
            {
                change.context = ctx.clone();
                // Явно извлекаем request_id из контекста
                change.request_id = if let Some(req_id) = ctx.get("request_id") {
                    req_id.as_str().map(|s| s.to_string())
                } else {
                    None
                };
            }
            change
        })
        .collect()
}

/// Сохраняет чанк данных в транзакции и фиксирует офсеты при успехе
async fn store_chunk_with_transaction(
    pool: &PgPool,
    chunk: &[ChangeDTO],
    consumer: &Arc<StreamConsumer>,
) -> Result<()> {
    // Начинаем транзакцию для записи в БД
    let mut tx = pool.begin().await?;

    match insert_rows_change(&mut tx, chunk).await {
        Ok(_) => {
            // Фиксируем транзакцию
            tx.commit().await?;

            // После успешной записи в БД фиксируем offset'ы
            if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                error!(error = %e, "Ошибка при коммите offset'а");
                // Не возвращаем ошибку здесь, т.к. данные уже сохранены
            }

            Ok(())
        }
        Err(e) => {
            error!(error = %e, "Ошибка при вставке данных в БД, откат транзакции");
            let _ = tx.rollback().await;
            Err(anyhow!("Ошибка сохранения данных: {}", e))
        }
    }
}

/// Функция для сборки батчей из Kafka
async fn kafka_batch_consumer(consumer: Arc<StreamConsumer>, sender: mpsc::Sender<Vec<ChangeDTO>>) {
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut consumer_stream = consumer.stream();

    // Добавить семафор для контроля кол-ва обрабатываемых батчей
    let semaphore = Arc::new(tokio::sync::Semaphore::new(CONCURRENT_PROCESSORS * 2));

    loop {
        // Пытаемся получить разрешение от семафора перед обработкой новых сообщений
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                error!("Семафор закрыт, завершаем обработку");
                break;
            }
        };

        tokio::select! {
            maybe_message = consumer_stream.next() => {
                match maybe_message {
                    Some(Ok(message)) => {
                        // Обрабатываем только где не пустой payload.
                        if let Some(payload) = message.payload() {
                            // Если произошла ошибка десериализации, сообщение отбрасывается
                            match payload_to_change_dto(payload) {
                                Ok(change) => batch.push(change),
                                Err(e) => error!(error=%e, "Ошибка при обработке сообщения! Сообщение пропущено!"),
                            }
                        }

                        if batch.len() >= BATCH_SIZE {
                            // Извлекает текущее содержимое переменной `batch, передавая его в новую переменную `batch_to_send`
                            // Заменяет содержимое переменной `batch` новым пустым вектором с заранее выделенной ёмкостью `BATCH_SIZE`
                            let batch_to_send = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                            let sender_clone = sender.clone();

                            tokio::spawn(async move {
                                if let Err(e) = sender_clone.send(batch_to_send).await {
                                    error!(error = %e, "Ошибка при отправке пачки");
                                }
                                // Разрешение освобождается автоматически при выходе из области видимости
                                drop(permit);
                            });
                        } else {
                            // Разрешение освобождается автоматически при выходе из области видимости
                            drop(permit);
                        }
                    }
                    Some(Err(e)) => {
                        error!(error = %e, "Ошибка при получении сообщения");
                    }
                    None => break,
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if !batch.is_empty() {
                    // Извлекает текущее содержимое переменной `batch, передавая его в новую переменную `batch_to_send`
                    // Заменяет содержимое переменной `batch` новым пустым вектором с заранее выделенной ёмкостью `BATCH_SIZE`
                    let batch_to_send = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                    if let Err(e) = sender.send(batch_to_send).await {
                        error!(error = %e, "Ошибка при отправке пачки");
                    }
                }

                drop(permit);
            }
        }
    }
}

/// Создание Kafka Consumer с ручным commit'ом offset'ов.
fn create_kafka_consumer(brokers: &str, group: &str) -> Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .create::<StreamConsumer>()
        .map_err(|err| anyhow!("KafkaError: {}", err))
}

/// Преобразование сообщения Kafka в Option<ChangeDTO>.
/// В случае ошибки десериализации возвращается None, чтобы не создавать "пустой" объект.
fn payload_to_change_dto(payload: &[u8]) -> Result<ChangeDTO> {
    let data: DebeziumData = serde_json::from_slice(payload)?;

    // Извлекаем primary_key для операций UPDATE и DELETE
    let primary_key = if matches!(data.operation, Operation::Update | Operation::Delete) {
        data.before.as_ref().and_then(|before| {
            before.as_object()?.get("id").map(|id| {
                // Удаляем кавычки из строки, если id является строкой
                if let Some(str_value) = id.as_str() {
                    str_value.to_string()
                } else {
                    // Иначе используем стандартное преобразование
                    id.to_string()
                }
            })
        })
    } else {
        None
    };

    let committed_at = DateTime::<Utc>::from_timestamp_millis(data.source.ts_ms)
        .ok_or_else(|| anyhow!("Не удалось получить timestamp для committed_at"))?;

    let queued_at = DateTime::<Utc>::from_timestamp_millis(data.ts_ms)
        .ok_or_else(|| anyhow!("Не удалось получить timestamp для queued_at"))?;

    let before = data.before.unwrap_or_else(|| serde_json::json!({}));
    let after = data.after.unwrap_or_else(|| serde_json::json!({}));

    // Обработка контекста: если префикс совпадает, декодируем base64
    let context = if let Some(m) = data.message {
        if m.prefix == PREFIX_AUDIT_CONTEXT {
            m.content
                .as_ref()
                .and_then(|encoded| {
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .map_err(|e| warn!(error=%e, "Ошибка декодирования base64!"))
                        .ok()
                })
                .and_then(|bytes: Vec<u8>| {
                    serde_json::from_slice::<serde_json::Value>(&bytes)
                        .map_err(|e| warn!(error=%e, "Ошибка декодирования JSON!"))
                        .ok()
                })
                .unwrap_or_else(|| serde_json::json!({}))
        } else {
            serde_json::json!({})
        }
    } else {
        serde_json::json!({})
    };

    // Попытка достать request_id
    let request_id = if let Some(req_id) = context.get("request_id") {
        req_id.as_str().map(|s| s.to_string())
    } else {
        None
    };

    Ok(ChangeDTO::new(
        data.source.db,
        data.source.schema,
        data.source.table,
        primary_key,
        data.operation.to_string(),
        before,
        after,
        context,
        request_id,
        committed_at,
        queued_at,
        Utc::now(),
        data.source.tx_id,
        data.source.lsn,
    ))
}
