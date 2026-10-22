//! Задача источника (ADR-0005): существующий цикл репликации,
//! параметризованный источником из реестра — бутстрап (публикация и слот
//! у клиента при подхвате), батчевая вставка в `changes` с ключом источника,
//! чекпоинт и ack слота только после коммита, реконнект с backoff.
//!
//! Ошибка источника не влияет на другие источники: она пишется в статус
//! реестра (`error` + текст в `last_error`), и задача повторяет попытку
//! с экспоненциальным backoff до остановки по токену.

use std::sync::Mutex;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::backoff;
use crate::bootstrap;
use crate::config::{
    BOOTSTRAP_TIMEOUT_SECS, DB_ACQUIRE_TIMEOUT_SECS, REPLICATION_SHUTDOWN_TIMEOUT_SECS,
    REPLICATION_START_TIMEOUT_SECS, SOURCE_CONNECT_TIMEOUT_SECS, WorkerConfig,
};
use crate::pool;
use crate::registry::{SourceRecord, StatusWriter};
use crate::replicate;

/// Размер пула задачи к БД аудита: вставка батчей + запись статусов.
const TASK_AUDIT_POOL_MAX_CONNECTIONS: u32 = 3;

/// Размер пула к источнику: только чтение каталога (первичные ключи).
const SOURCE_POOL_MAX_CONNECTIONS: u32 = 2;

/// Взведённая тестовая паника: имя источника и сколько ещё запусков его
/// задачи запаникуют (каждый запуск расходует одну). Шов живых тестов
/// самовосстановления супервизора (тикет #47).
type ArmedTaskPanic = Option<(String, u32)>;
static TEST_TASK_PANIC: Mutex<ArmedTaskPanic> = Mutex::new(None);

/// Взводит одноразовую панику задачи источника: следующие `times` запусков
/// задачи источника с именем `source` паникуют до каких-либо подключений.
/// Только для живых тестов супервизора (тикет #47): в продукте паник не
/// взводят, шов позволяет детерминированно проверить цикл «паника →
/// снятие с учёта → перезапуск» без инжекции в реальный тракт репликации.
pub fn arm_task_panic(source: &str, times: u32) {
    *TEST_TASK_PANIC
        .lock()
        .expect("мьютекс тестового шва паники не отравлен") = Some((source.to_owned(), times));
}

/// Расходует взведённую панику: `true` — этот запуск задачи должен
/// запаниковать.
fn test_panic_due(source: &str) -> bool {
    let mut armed = TEST_TASK_PANIC
        .lock()
        .expect("мьютекс тестового шва паники не отравлен");
    match armed.as_mut() {
        Some((name, left)) if name == source && *left > 0 => {
            *left -= 1;
            true
        }
        _ => false,
    }
}

/// Чем закончилась попытка источника (подключение + сессия репликации).
enum AttemptEnd {
    /// Остановка по `CancellationToken`: задача источника завершена.
    Cancelled,
    /// Попытка не удалась: статус ошибки в реестр, повтор с backoff.
    Failed(anyhow::Error),
}

/// Крутит цикл репликации одного источника до остановки по токену.
///
/// Каждая попытка: подключение к клиенту, бутстрап (публикация и слот
/// создаются на клиенте при подхвате — ADR-0005), подключение к
/// репликации, сессия потока. Неудача — статус `error` в реестре и
/// повтор с экспоненциальным backoff. Точка возобновления — чекпоинт
/// слота в БД аудита; replay-дубли гасятся дедупликацией по уникальному
/// ключу `("source", commit_lsn, event_index)`.
///
/// Остановки: токен (graceful shutdown воркера, точечная остановка задачи
/// супервизором) или канал `supervisor_gone` — задача не переживает своего
/// супервизора, при его аварийной остановке соединение закрывается,
/// сохранённый прогресс остаётся в чекпоинтах.
pub(crate) async fn run(
    config: WorkerConfig,
    record: SourceRecord,
    token: CancellationToken,
    mut supervisor_gone: watch::Receiver<()>,
) {
    // Тестовый шов (тикет #47): паника до любых ресурсов задачи — ни одного
    // соединения ещё не открыто, падение безопасно и детерминированно.
    if test_panic_due(&record.name) {
        panic!("тестовая паника задачи источника ({})", record.name);
    }
    // Пул к БД аудита принадлежит задаче: батчи источников не делят
    // соединения, медленный клиент не занимает чужие подключения.
    // Статусы пишутся в тот же пул (заимствование): пул — ресурс задачи,
    // статус — лишь один его потребитель.
    let Some(audit) = pool::audit_pool_with_retry(
        &config,
        TASK_AUDIT_POOL_MAX_CONNECTIONS,
        &record.name,
        "задача",
        &token,
        &mut supervisor_gone,
    )
    .await
    else {
        return;
    };
    let status = StatusWriter::new(&audit, record.name.clone());

    status.starting().await;
    info!(
        source = %record.name,
        slot = %record.slot(),
        publication = %record.publication(),
        "задача источника запущена"
    );

    // Счётчик подряд идущих неудач источника: сбрасывается каждой
    // удавшейся попыткой, задаёт экспоненту backoff.
    let mut attempt: u32 = 0;
    loop {
        if token.is_cancelled() || supervisor_gone.has_changed().is_err() {
            break;
        }
        tokio::select! {
            _ = token.cancelled() => break,
            // Супервизор исчез (аварийная остановка): сессия обрывается
            // вместе с будущим попытки, сохранённый прогресс — в чекпоинтах.
            _ = supervisor_gone.changed() => break,
            outcome = run_attempt(&record, &audit, &status, &token, &config, &mut attempt) => {
                match outcome {
                    AttemptEnd::Cancelled => break,
                    AttemptEnd::Failed(e) => {
                        status.error(&e).await;
                        attempt += 1;
                        if !backoff_after_failure(
                            &token,
                            &mut supervisor_gone,
                            &config,
                            attempt,
                            &record.name,
                            &e,
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
            }
        }
    }

    info!(source = %record.name, "задача источника остановлена");
}

/// Единая реакция на неудачную попытку: warn-лог с причиной и пауза backoff,
/// прерываемая остановкой (токен или исчезновение супервизора). `false` —
/// остановка пришла в паузе.
async fn backoff_after_failure(
    token: &CancellationToken,
    supervisor_gone: &mut watch::Receiver<()>,
    config: &WorkerConfig,
    attempt: u32,
    source: &str,
    error: &anyhow::Error,
) -> bool {
    warn!(
        source,
        attempt,
        reason = "попытка источника не удалась — повтор с backoff",
        error = %error,
    );
    backoff::pause(
        token,
        supervisor_gone,
        backoff::reconnect_delay(config, attempt),
    )
    .await
}

/// Одна попытка источника: подключение, бутстрап, сессия репликации.
/// Подключение к БД источника, бутстрап и подтверждение старта репликации
/// ограничены таймаутами (config.rs): зависание сети переводит попытку
/// в видимый `error` с backoff вместо вечного `starting` (тикет #46).
/// Потолок буфера транзакции — настройка воркера, передаётся в сессию
/// (тикет #48).
async fn run_attempt(
    record: &SourceRecord,
    audit: &sqlx::PgPool,
    status: &StatusWriter<'_>,
    token: &CancellationToken,
    config: &WorkerConfig,
    attempt: &mut u32,
) -> AttemptEnd {
    // Пул источника живёт одну попытку: пересоздаётся после обрыва.
    // Внешний таймаут страхует и фазу DNS/TCP, которую acquire_timeout
    // пула оборвать не может.
    let source = match tokio::time::timeout(
        Duration::from_secs(SOURCE_CONNECT_TIMEOUT_SECS),
        PgPoolOptions::new()
            .max_connections(SOURCE_POOL_MAX_CONNECTIONS)
            .acquire_timeout(Duration::from_secs(DB_ACQUIRE_TIMEOUT_SECS))
            .connect(&record.url),
    )
    .await
    {
        Ok(Ok(pool)) => pool,
        Ok(Err(e)) => {
            return AttemptEnd::Failed(anyhow::Error::new(e).context("подключение к БД источника"));
        }
        Err(_) => {
            return AttemptEnd::Failed(anyhow::anyhow!(
                "подключение к БД источника не уложилось в {SOURCE_CONNECT_TIMEOUT_SECS} с"
            ));
        }
    };

    let boot = match tokio::time::timeout(
        Duration::from_secs(BOOTSTRAP_TIMEOUT_SECS),
        bootstrap::bootstrap(audit, &source, record),
    )
    .await
    {
        Ok(Ok(boot)) => boot,
        Ok(Err(e)) => return AttemptEnd::Failed(e.context("бутстрап источника не удался")),
        Err(_) => {
            return AttemptEnd::Failed(anyhow::anyhow!(
                "бутстрап источника не уложился в {BOOTSTRAP_TIMEOUT_SECS} с"
            ));
        }
    };
    info!(
        source = %record.name,
        source_database = %boot.source_database,
        start_lsn = %boot.start_lsn,
        "бутстрап источника завершён, подключаемся к репликации"
    );

    let mut client = match replicate::connect_replication(record, boot.start_lsn).await {
        Ok(client) => client,
        Err(e) => return AttemptEnd::Failed(e),
    };
    // Подключение walsender фоновое: TCP, аутентификация и
    // START_REPLICATION завершаются не позже подтверждения старта —
    // таймаут на нём ограничивает весь тракт подключения.
    let started = tokio::time::timeout(
        Duration::from_secs(REPLICATION_START_TIMEOUT_SECS),
        client.wait_started(),
    )
    .await;
    match started {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            // Например, слот занят другим консюмером: повтор с backoff.
            return AttemptEnd::Failed(
                anyhow::Error::new(e).context("старт репликации не подтверждён сервером"),
            );
        }
        Err(_) => {
            return AttemptEnd::Failed(anyhow::anyhow!(
                "старт репликации не подтвердился за {REPLICATION_START_TIMEOUT_SECS} с"
            ));
        }
    }
    // Подключение и старт удались: счётчик неудач сбрасывается.
    *attempt = 0;
    status.connected().await;
    info!(source = %record.name, "репликация источника запущена");

    let end = replicate::stream_session(
        &mut client,
        audit,
        &source,
        &boot,
        record,
        status,
        token,
        config.tx_buffer_max_rows,
    )
    .await;
    if matches!(end, replicate::SessionEnd::Cancelled) {
        // Остановка обязана уложиться в grace-окно оркестратора: на
        // зависшей сети закрытие обрывается по таймауту, соединение
        // добивает дроп клиента.
        let shutdown = tokio::time::timeout(
            Duration::from_secs(REPLICATION_SHUTDOWN_TIMEOUT_SECS),
            client.shutdown(),
        )
        .await;
        match shutdown {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!(
                source = %record.name,
                error = %e,
                "штатное закрытие репликации завершилось с ошибкой"
            ),
            Err(_) => warn!(
                source = %record.name,
                "закрытие репликации не уложилось в таймаут остановки — соединение оставлено на дроп"
            ),
        }
    }
    match end {
        replicate::SessionEnd::Cancelled => AttemptEnd::Cancelled,
        replicate::SessionEnd::Reconnect(e) => AttemptEnd::Failed(e),
    }
}
