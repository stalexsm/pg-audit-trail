//! Супервизор воркера (ADR-0005): периодически опрашивает реестр источников
//! в БД аудита и держит по задаче на каждый источник. Зарегистрированные
//! источники подхватываются без перезапуска воркера; задачи источников,
//! помеченных к удалению или исчезнувших из реестра, останавливаются, а для
//! помеченных запускается исполнение удаления: дроп слота и публикации у
//! клиента, затем снятие строки реестра (тикет #31). Правки настроек живого
//! источника применяются перезапуском задачи. Остановка воркера — по общему
//! `CancellationToken`: супервизор отменяет дочерние токены задач и ждёт
//! завершения задач и исполнений удаления.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::Result;
use crate::config::{DB_ACQUIRE_TIMEOUT_SECS, WorkerConfig};
use crate::deletion;
use crate::metrics;
use crate::registry::{self, SourceRecord, StatusWriter};
use crate::source_task;

/// Интервал опроса реестра источников, секунды: определяет задержку
/// подхвата новых источников и реакции на правки и удаления.
const REGISTRY_POLL_INTERVAL_SECS: u64 = 5;

/// Размер пула супервизора к БД аудита: только чтение реестра.
const REGISTRY_POOL_MAX_CONNECTIONS: u32 = 2;

/// Задача источника под управлением супервизора: дочерний токен для
/// точечной остановки и снимок настроек на момент запуска (для реакции
/// на правки реестра).
struct SourceHandle {
    record: SourceRecord,
    token: CancellationToken,
    handle: JoinHandle<()>,
}

/// Запускает воркер CDC до отмены `CancellationToken`.
///
/// Применяет миграции к БД аудита, опрашивает реестр источников и держит
/// по задаче репликации на каждый источник (ADR-0005): каждая задача сама
/// бутстрапит публикацию и слот у клиента, пишет батчи в `changes` с ключом
/// источника, ведёт чекпоинт и статус источника в реестре. Сводные метрики
/// конвейера — по процессу (все источники вместе).
pub async fn run(config: WorkerConfig, token: CancellationToken) -> Result<()> {
    let audit = PgPoolOptions::new()
        .max_connections(REGISTRY_POOL_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(DB_ACQUIRE_TIMEOUT_SECS))
        .connect(&config.database_url)
        .await
        .context("подключение к БД аудита")?;
    sqlx::migrate!("../../migrations")
        .run(&audit)
        .await
        .context("миграции БД аудита")?;

    info!(
        poll_interval_secs = REGISTRY_POLL_INTERVAL_SECS,
        "супервизор воркера запущен: источники читаются из реестра (ADR-0005)"
    );

    // Канал «супервизор жив»: задачи источников не переживают своего
    // супервизора. При graceful shutdown задачи останавливаются токенами;
    // при аварийной остановке (abort/panic супервизора) отправитель
    // удаляется вместе с его future, и задачи завершаются по каналу —
    // без него сироты держали бы слоты и вмешивались в новые задачи.
    // Отправитель живёт до конца run (префикс «_» — только чтобы смолчал
    // warning о неиспользованности).
    let (_supervisor_gone_tx, supervisor_gone_rx) = watch::channel(());

    let reporter = tokio::spawn(metrics::report_loop(token.clone()));

    let mut tasks: HashMap<String, SourceHandle> = HashMap::new();
    // Исполнения удаления помеченных к удалению источников (тикет #31);
    // остановка — вместе с супервизором (общий токен воркера).
    let mut deletions: HashMap<String, JoinHandle<()>> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(REGISTRY_POLL_INTERVAL_SECS));
    // Пропущенные (пока шёл предыдущий опрос) тики не догоняются.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Первый тик interval срабатывает сразу: подхват не ждёт интервала.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("получен сигнал остановки: останавливаем задачи источников");
                break;
            }
            _ = ticker.tick() => {
                reconcile(
                    &audit,
                    &config,
                    &token,
                    &supervisor_gone_rx,
                    &mut tasks,
                    &mut deletions,
                )
                .await
            }
        }
    }

    // Остановка задач: у каждой дочерний токен; супервизор ждёт завершения,
    // чтобы при graceful shutdown сохранённый прогресс остался в чекпоинтах.
    for handle in tasks.values() {
        handle.token.cancel();
    }
    for (_, handle) in tasks.drain() {
        if let Err(e) = handle.handle.await {
            warn!(error = %e, "задача источника завершилась паникой");
        }
    }

    // Исполнения удаления останавливаются общим токеном воркера; незавершённое
    // удаление остаётся помеченным в реестре — исполнение продолжит новый воркер.
    for (_, handle) in deletions.drain() {
        if let Err(e) = handle.await {
            warn!(error = %e, "исполнение удаления завершилось паникой");
        }
    }

    reporter.abort();
    info!("супервизор остановлен: все задачи источников завершены");
    Ok(())
}

/// Один такт согласования запущенных задач с реестром: запуск задач новым
/// источникам, остановка задач удалённых и исчезнувших, исполнение удаления
/// помеченных, перезапуск задач с изменившимися настройками.
async fn reconcile(
    audit: &PgPool,
    config: &WorkerConfig,
    token: &CancellationToken,
    supervisor_gone_rx: &watch::Receiver<()>,
    tasks: &mut HashMap<String, SourceHandle>,
    deletions: &mut HashMap<String, JoinHandle<()>>,
) {
    let sources = match registry::list_sources(audit).await {
        Ok(sources) => sources,
        Err(e) => {
            warn!(error = %e, "опрос реестра источников не удался — перенос на следующий тик");
            return;
        }
    };

    // Завершившиеся исполнения удаления снимаются с учёта.
    let finished: Vec<String> = deletions
        .iter()
        .filter(|(_, handle)| handle.is_finished())
        .map(|(name, _)| name.clone())
        .collect();
    for name in finished {
        if let Some(handle) = deletions.remove(&name)
            && let Err(e) = handle.await
        {
            warn!(source = %name, error = %e, "исполнение удаления завершилось паникой");
        }
    }

    // Завершившиеся сами по себе задачи источников (паника, преждевременный
    // выход) снимаются с учёта: статус в реестре отражает сбой, шаг запуска
    // ниже перезапускает задачу без рестарта процесса (тикет #47).
    reap_finished_tasks(audit, tasks).await;

    // Активные источники: не помеченные к удалению.
    let active: HashMap<&str, &SourceRecord> = sources
        .iter()
        .filter(|record| record.deleted_at.is_none())
        .map(|record| (record.name.as_str(), record))
        .collect();

    // Помеченные к удалению: задача останавливается сразу, чтобы слот
    // отпустил WAL; затем исполняется удаление — дроп слота и публикации
    // у клиента, снятие чекпоинта и строки реестра (тикет #31). Клиент
    // недоступен — исполнение повторяет с backoff, состояние застрявшего
    // удаления видно в реестре, пока дроп не удался.
    for record in sources.iter().filter(|record| record.deleted_at.is_some()) {
        if tasks.contains_key(&record.name) {
            info!(source = %record.name, "источник помечен к удалению — задача остановлена");
            stop_task(tasks, &record.name).await;
        }
        if !deletions.contains_key(&record.name) {
            info!(
                source = %record.name,
                "источник помечен к удалению — исполняем дроп слота и публикации"
            );
            spawn_deletion(record, config, token, supervisor_gone_rx.clone(), deletions);
        }
    }

    // Исчезнувшие из реестра (строку сняли в обход удаления): остановить задачу.
    let vanished: Vec<String> = tasks
        .keys()
        .filter(|name| !active.contains_key(name.as_str()))
        .cloned()
        .collect();
    for name in vanished {
        info!(source = %name, "источник исчез из реестра — задача остановлена");
        stop_task(tasks, &name).await;
    }

    // Новые источники: запустить задачи.
    for (name, record) in &active {
        if !tasks.contains_key(*name) {
            info!(source = %name, "новый источник в реестре — запускаем задачу");
            spawn_task(record, config, token, supervisor_gone_rx.clone(), tasks);
        }
    }

    // Правки настроек живого источника: перезапуск задачи (ADR-0005).
    let edited: Vec<&SourceRecord> = active
        .values()
        .filter(|record| {
            tasks
                .get(record.name.as_str())
                .is_some_and(|handle| handle.record.settings_differ(record))
        })
        .copied()
        .collect();
    for record in edited {
        info!(source = %record.name, "настройки источника изменились — перезапуск задачи");
        stop_task(tasks, &record.name).await;
        spawn_task(record, config, token, supervisor_gone_rx.clone(), tasks);
    }
}

/// Останавливает задачу источника: отмена дочернего токена и ожидание
/// завершения — чекпоинты сохранены, слот отпущен.
async fn stop_task(tasks: &mut HashMap<String, SourceHandle>, name: &str) {
    if let Some(handle) = tasks.remove(name) {
        handle.token.cancel();
        if let Err(e) = handle.handle.await {
            warn!(source = name, error = %e, "задача источника завершилась паникой");
        }
    }
}

/// Снимает с учёта задачи источников, завершившиеся сами по себе (по образцу
/// очистки исполнений удаления): паника или преждевременный выход без сигнала
/// остановки диагностируются, диагноз идёт в лог и в статус источника в
/// реестре (`error` + текст в `last_error` — реестр не показывает устаревшее
/// `connected`), задача снимается с учёта — шаг запуска того же такта
/// перезапускает её без рестарта процесса (тикет #47).
async fn reap_finished_tasks(audit: &PgPool, tasks: &mut HashMap<String, SourceHandle>) {
    let finished: Vec<String> = tasks
        .iter()
        .filter(|(_, handle)| handle.handle.is_finished())
        .map(|(name, _)| name.clone())
        .collect();
    for name in finished {
        let Some(handle) = tasks.remove(&name) else {
            continue;
        };
        let diagnosis = abnormal_exit_diagnosis(handle.handle.await);
        warn!(
            source = %name,
            "{diagnosis} — задача снята с учёта и будет перезапущена на следующем такте"
        );
        StatusWriter::new(audit, name.clone())
            .error(&anyhow::anyhow!("{diagnosis}"))
            .await;
    }
}

/// Диагноз нештатного завершения задачи по результату её JoinHandle:
/// паника — с текстом payload (Display JoinError текст не несёт),
/// иначе — завершение без сигнала остановки (задача живёт до остановки
/// воркера). Текст идёт в лог и в `last_error` статуса реестра.
fn abnormal_exit_diagnosis(result: std::result::Result<(), tokio::task::JoinError>) -> String {
    match result {
        Ok(()) => "задача источника завершилась сама без сигнала остановки".to_owned(),
        Err(join) if join.is_panic() => {
            let message = match join.try_into_panic() {
                Ok(payload) => match payload.downcast_ref::<&str>() {
                    Some(text) => (*text).to_owned(),
                    None => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .unwrap_or_else(|| "паника без текста".to_owned()),
                },
                Err(join) => format!("{join}"),
            };
            format!("задача источника завершилась паникой: {message}")
        }
        Err(join) => format!("задача источника завершилась с ошибкой: {join}"),
    }
}

/// Запускает задачу источника с дочерним токеном и снимком настроек.
/// Задача создаёт свои пулы к обеим БД сама (изоляция медленного клиента)
/// и останавливается вместе с супервизором (канал `supervisor_gone`).
fn spawn_task(
    record: &SourceRecord,
    config: &WorkerConfig,
    token: &CancellationToken,
    supervisor_gone: watch::Receiver<()>,
    tasks: &mut HashMap<String, SourceHandle>,
) {
    let task_token = token.child_token();
    let handle = tokio::spawn(source_task::run(
        config.clone(),
        record.clone(),
        task_token.clone(),
        supervisor_gone,
    ));
    tasks.insert(
        record.name.clone(),
        SourceHandle {
            record: record.clone(),
            token: task_token,
            handle,
        },
    );
}

/// Запускает исполнение удаления источника (тикет #31): дроп слота и
/// публикации у клиента, затем снятие строки реестра. Исполнение
/// останавливается вместе с супервизором: общий токен воркера и канал
/// `supervisor_gone` (не переживает аварийной остановки супервизора).
fn spawn_deletion(
    record: &SourceRecord,
    config: &WorkerConfig,
    token: &CancellationToken,
    supervisor_gone: watch::Receiver<()>,
    deletions: &mut HashMap<String, JoinHandle<()>>,
) {
    let handle = tokio::spawn(deletion::run(
        config.clone(),
        record.clone(),
        token.clone(),
        supervisor_gone,
    ));
    deletions.insert(record.name.clone(), handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str) -> SourceRecord {
        SourceRecord {
            name: name.to_owned(),
            url: "postgresql://u:p@h:5432/db".to_owned(),
            ignore_tables: Vec::new(),
            message_prefix: "_audit".to_owned(),
            batch_size: 500,
            deleted_at: None,
        }
    }

    /// Ленивый пул в никуда: тест проверяет снятие задачи с учёта, запись
    /// статуса в реестр (StatusWriter) при недоступной БД лишь логирует warn.
    fn audit_lazy() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://invalid:invalid@127.0.0.1:1/none")
            .expect("ленивый пул создаётся без подключения")
    }

    /// Паника задачи диагностируется с текстом payload: текст идёт в лог
    /// и в `last_error` статуса реестра (тикет #47).
    #[tokio::test]
    async fn panic_diagnosis_carries_payload_text() {
        let handle = tokio::spawn(async {
            panic!("сломанный источник");
        });
        let join_error = handle
            .await
            .expect_err("задача с panic! завершается JoinError");

        let diagnosis = abnormal_exit_diagnosis(Err(join_error));
        assert!(
            diagnosis.contains("паникой"),
            "диагноз называет панику: {diagnosis}"
        );
        assert!(
            diagnosis.contains("сломанный источник"),
            "текст payload паники попадает в диагноз: {diagnosis}"
        );
    }

    /// Преждевременный выход задачи без сигнала остановки — тоже нештатное
    /// завершение: задача живёт до остановки воркера.
    #[tokio::test]
    async fn premature_exit_gets_diagnosis() {
        let handle = tokio::spawn(async {});
        let diagnosis = abnormal_exit_diagnosis(handle.await);
        assert!(
            diagnosis.contains("без сигнала остановки"),
            "диагноз называет преждевременный выход: {diagnosis}"
        );
    }

    /// AC (тикет #47): завершившаяся паникой задача снимается с учёта —
    /// шаг запуска супервизора перезапустит её без рестарта процесса;
    /// работающая задача не трогается.
    #[tokio::test]
    async fn crashed_task_is_reaped_and_healthy_task_stays() {
        let audit = audit_lazy();
        let mut tasks: HashMap<String, SourceHandle> = HashMap::new();

        let crashed = record("crashed_src");
        tasks.insert(
            crashed.name.clone(),
            SourceHandle {
                record: crashed,
                token: CancellationToken::new(),
                handle: tokio::spawn(async {
                    panic!("тестовая паника задачи источника");
                }),
            },
        );

        let healthy = record("healthy_src");
        let healthy_token = CancellationToken::new();
        tasks.insert(
            healthy.name.clone(),
            SourceHandle {
                record: healthy,
                token: healthy_token.clone(),
                handle: tokio::spawn(std::future::pending()),
            },
        );

        // Ждём фактического завершения паникующей задачи.
        while !tasks["crashed_src"].handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        reap_finished_tasks(&audit, &mut tasks).await;

        assert!(
            !tasks.contains_key("crashed_src"),
            "упавшая задача снята с учёта — следующий такт её перезапустит"
        );
        assert!(
            tasks.contains_key("healthy_src"),
            "работающая задача не тронута"
        );
        assert!(
            !healthy_token.is_cancelled(),
            "токен работающей задачи не отменён"
        );
    }
}
