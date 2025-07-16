use futures::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::BorrowedMessage,
};
use redis::{AsyncCommands, aio::MultiplexedConnection};
use serde::Deserialize;
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::{
    Result,
    database::{AuditDTO, insert_rows_audit},
};

#[derive(Debug, Deserialize)]
pub struct AppData {
    pub prefix: String,
    pub status_code: i16,
    pub event: String,
    pub primary_key: Option<String>,
    pub response_body: String,
    pub context: serde_json::Value,
}

pub async fn run_loop(
    pool: PgPool,
    redis: redis::Client,
    brokers: String,
    group: String,
    topic: String,
) -> Result<()> {
    let group = format!("{group}_app");

    info!(topic = topic, group = group, "Запуск обработки App!");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .create()?;

    // Подписка на темы
    consumer.subscribe(&[&topic])?;

    const BATCH_SIZE: usize = 100;
    const MAX_POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

    let mut consumer_msg_stream = consumer.stream();
    let mut con = redis.get_multiplexed_async_connection().await?;

    loop {
        let mut messages = Vec::with_capacity(BATCH_SIZE);
        let timeout = tokio::time::sleep(MAX_POLL_TIMEOUT);

        tokio::pin!(timeout);
        // Собираем сообщения до таймаута или достижения размера батча
        loop {
            tokio::select! {
                _ = &mut timeout => break,
                borrowed_message = consumer_msg_stream.next() => {
                    if let Some(msg) = borrowed_message {
                        match msg {
                            Ok(msg) => {
                                if messages.len() >= BATCH_SIZE {
                                    break;
                                }
                                messages.push(msg);
                            }
                            Err(err) => {
                                error!(error=%err,"Ошибка получения сообщения!");
                            }
                        }

                    }
                }
            }
        }

        if messages.is_empty() {
            continue;
        }

        info!(message_cnt = messages.len(), "Сообщений для обработки!");

        // Обработка данных
        let records = borrowed_message_to_audit_dto(&mut con, &messages).await?;

        // Обработка изменений в бд и удаление записей из буфера
        let mut tx = pool.begin().await?;
        if let Err(err) = insert_rows_audit(&mut tx, &records).await {
            error!(error=%err, "Произошла ошибка при обработке изменений в БД");
        } else {
            // Коммит транзакции
            if let Err(err) = tx.commit().await {
                error!(error=%err, "Произошла ошибка при подтверждении транзакции", );
            } else {
                // Commit the messages
                for message in messages {
                    if let Err(err) = consumer.commit_message(&message, CommitMode::Async) {
                        error!(error=%err, "Произошла ошибка при подтверждении сообщения в Kafka");
                    }
                }
            }
        }
    }
}

async fn borrowed_message_to_audit_dto(
    con: &mut MultiplexedConnection,
    messages: &[BorrowedMessage<'_>],
) -> Result<Vec<AuditDTO>> {
    // Обработка сообщения

    let mut audit_dto_vec = Vec::with_capacity(messages.len());
    for message in messages {
        let payload = match message.payload() {
            Some(data) => data,
            None => {
                warn!("Пустое тело payload!");
                continue;
            }
        };

        match serde_json::from_slice::<AppData>(payload) {
            Ok(payload) => {
                let mut response_body = String::new();
                if !payload.response_body.is_empty() {
                    response_body = match con.get_del(&payload.response_body).await {
                        Ok(body) => body,
                        Err(e) => {
                            error!(error=%e, "Ошибка при получении body из redis!");
                            String::new()
                        }
                    };
                }

                // Попытка получить ID (request_id) из контекста
                let id = payload
                    .context
                    .get("request_id")
                    .and_then(|request_id| request_id.as_str())
                    .and_then(|str_id| uuid::Uuid::parse_str(str_id).ok())
                    .unwrap_or_else(uuid::Uuid::new_v4);

                let audit_dto = AuditDTO::new(
                    id,
                    payload.event,
                    payload.primary_key,
                    payload.status_code,
                    response_body,
                    payload.context,
                );

                audit_dto_vec.push(audit_dto);
            }
            Err(e) => {
                error!(error=%e, "Ошибка декодирования сообщения JSON!");
                continue;
            }
        };
    }

    Ok(audit_dto_vec)
}
