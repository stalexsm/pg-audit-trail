//! Реестр источников (ADR-0005): чтение списка источников и запись их
//! статусов воркером. Единственный источник правды о настройках источника —
//! таблица `sources` в БД аудита; строки подключения, служебные таблицы,
//! префикс MESSAGE и размер батча из окружения не читаются.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::warn;

use crate::Result;

/// Интервал обновления `last_event_at` в реестре: обновление на каждый
/// коммит источника удвоило бы нагрузку на БД аудита, поэтому статус
/// последнего события пишется не чаще указанного интервала.
const LAST_EVENT_WRITE_INTERVAL: Duration = Duration::from_secs(5);

/// Строка реестра источников, нужная задаче воркера (ADR-0005).
#[derive(Debug, Clone)]
pub(crate) struct SourceRecord {
    pub name: String,
    /// Строка подключения к базе клиента с кредами; неизменяема (ADR-0005).
    pub url: String,
    /// Служебные таблицы источника в виде `schema.table`, не попадающие в журнал.
    pub ignore_tables: Vec<String>,
    /// Префикс MESSAGE, интерпретируемого как канал контекста транзакции.
    pub message_prefix: String,
    /// Размер чанка батчевой вставки строк в журнал.
    pub batch_size: usize,
    /// Когда источник помечен к удалению; `None` — не помечен.
    pub deleted_at: Option<DateTime<Utc>>,
}

impl SourceRecord {
    /// Слот логической репликации источника: `waltrail_<имя>` (ADR-0005).
    /// Имя чекпоинта совпадает со слотом — чекпоинты источников не пересекаются.
    pub(crate) fn slot(&self) -> String {
        format!("waltrail_{}", self.name)
    }

    /// Публикация источника на клиенте: `audit_<имя>` (ADR-0005).
    pub(crate) fn publication(&self) -> String {
        format!("audit_{}", self.name)
    }

    /// Изменились ли настройки, которые правятся у живого источника
    /// (PATCH реестра): служебные таблицы, префикс MESSAGE, размер батча.
    /// Имя и URL неизменяемы, статус воркера настройкой не является.
    pub(crate) fn settings_differ(&self, other: &SourceRecord) -> bool {
        self.ignore_tables != other.ignore_tables
            || self.message_prefix != other.message_prefix
            || self.batch_size != other.batch_size
    }
}

/// Строка реестра как читается из БД: размер батча — i32 (INT), статус
/// удаления — метка времени.
type SourceRow = (
    String,
    String,
    Vec<String>,
    String,
    i32,
    Option<DateTime<Utc>>,
);

/// Читает реестр источников, по имени. Ошибка чтения возвращается наружу:
/// супервизор переносит опрос на следующий тик, не трогая запущенные задачи.
pub(crate) async fn list_sources(audit: &PgPool) -> Result<Vec<SourceRecord>> {
    let rows: Vec<SourceRow> = sqlx::query_as(
        "SELECT name, url, ignore_tables, message_prefix, batch_size, deleted_at \
             FROM sources ORDER BY name",
    )
    .fetch_all(audit)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(name, url, ignore_tables, message_prefix, batch_size, deleted_at)| SourceRecord {
                // Границы размера батча проверяет API (1..=4095 — бюджет
                // параметров statement в ingest.rs); защита от нуля и
                // отрицательных — на случай правок реестра в обход API,
                // значения выше бюджета сжимает нарезка чанков вставки.
                batch_size: usize::try_from(batch_size.max(1)).unwrap_or(1),
                name,
                url,
                ignore_tables,
                message_prefix,
                deleted_at,
            },
        )
        .collect())
}

/// Состояния задачи воркера в реестре (`sources.worker_state`).
/// `pending` (дефолт схемы) ставится при регистрации — источник ждёт подхвата.
pub(crate) mod worker_state {
    /// Задача подхвачена: бутстрап (публикация и слот у клиента) и подключение.
    pub(crate) const STARTING: &str = "starting";

    /// Сессия репликации активна.
    pub(crate) const CONNECTED: &str = "connected";

    /// Последняя попытка не удалась, текст — в `last_error`; задача повторяет с backoff.
    pub(crate) const ERROR: &str = "error";

    /// Исполняется удаление: дроп слота и публикации у клиента пока не удался;
    /// текст последней неудачи — в `last_error`. Состояние застрявшего удаления
    /// видно в реестре, пока дроп не удался (тикет #31).
    pub(crate) const DELETING: &str = "deleting";
}

/// Неудачная запись статуса в реестр не роняет задачу — только warn.
fn warn_status_not_written(source: &str, state: &str, error: &sqlx::Error) {
    warn!(
        source,
        state,
        error = %error,
        "статус источника в реестр не записан"
    );
}

/// Пишет статус источника в реестр (`worker_state`, `last_error`,
/// `last_event_at`). Пул заимствуется у задачи: пул — ресурс задачи,
/// статус — один из его потребителей.
///
/// Ошибки записи статуса не роняют задачу источника: статус — вспомогательные
/// данные для наблюдения, ошибка пишется в лог. Поле `updated_at` статусы
/// не трогают: оно про правки настроек, а не про активность воркера.
pub(crate) struct StatusWriter<'a> {
    audit: &'a PgPool,
    name: String,
    /// Момент последней записи `last_event_at`: держит троттлинг.
    last_event_write: Mutex<Instant>,
}

impl<'a> StatusWriter<'a> {
    pub(crate) fn new(audit: &'a PgPool, name: String) -> Self {
        // Сдвиг в прошлое гарантирует запись первого события без паузы.
        let started = Instant::now();
        let last_event_write = started
            .checked_sub(LAST_EVENT_WRITE_INTERVAL)
            .unwrap_or(started);
        Self {
            audit,
            name,
            last_event_write: Mutex::new(last_event_write),
        }
    }

    async fn write(&self, state: &str, error: Option<&str>) {
        if let Err(e) =
            sqlx::query("UPDATE sources SET worker_state = $2, last_error = $3 WHERE name = $1")
                .bind(&self.name)
                .bind(state)
                .bind(error)
                .execute(self.audit)
                .await
        {
            warn_status_not_written(&self.name, state, &e);
        }
    }

    /// Записывает только `worker_state`, не трогая `last_error`
    /// (контракт сохранения текста последней ошибки — у `starting`).
    async fn write_state(&self, state: &str) {
        if let Err(e) = sqlx::query("UPDATE sources SET worker_state = $2 WHERE name = $1")
            .bind(&self.name)
            .bind(state)
            .execute(self.audit)
            .await
        {
            warn_status_not_written(&self.name, state, &e);
        }
    }

    /// Задача подхвачена: бутстрап и подключение. Прошлый `last_error`
    /// сохраняется до первого успеха — «текст последней ошибки»: диагноз
    /// аварийного перезапуска виден в реестре, пока источник не подключился.
    pub(crate) async fn starting(&self) {
        self.write_state(worker_state::STARTING).await;
    }

    /// Сессия репликации активна: последняя ошибка закрыта.
    pub(crate) async fn connected(&self) {
        self.write(worker_state::CONNECTED, None).await;
    }

    /// Попытка не удалась: состояние и текст ошибки в реестре.
    pub(crate) async fn error(&self, error: &anyhow::Error) {
        self.write(worker_state::ERROR, Some(&format!("{error:#}")))
            .await;
    }

    /// Удаление исполняется и пока не удалось: состояние застрявшего
    /// удаления в реестре, текст последней неудачи — в `last_error`.
    /// `None` — удаление начато, прошлый текст ошибки закрывается.
    pub(crate) async fn deleting(&self, error: Option<&anyhow::Error>) {
        let text = error.map(|e| format!("{e:#}"));
        self.write(worker_state::DELETING, text.as_deref()).await;
    }

    /// Изменения источника сохранены в журнал: обновляет `last_event_at` —
    /// не чаще `LAST_EVENT_WRITE_INTERVAL`, чтобы не удваивать нагрузку
    /// на БД аудита на горячем пути.
    pub(crate) async fn event_saved(&self) {
        let due = {
            let mut last = self
                .last_event_write
                .lock()
                .expect("мьютекс троттлинга статуса не отравлен");
            let now = Instant::now();
            if event_write_due(*last, now) {
                *last = now;
                true
            } else {
                false
            }
        };
        if !due {
            return;
        }
        if let Err(e) = sqlx::query("UPDATE sources SET last_event_at = now() WHERE name = $1")
            .bind(&self.name)
            .execute(self.audit)
            .await
        {
            warn!(source = %self.name, error = %e, "last_event_at в реестр не записан");
        }
    }
}

/// Пора ли обновлять `last_event_at`: не чаще интервала троттлинга.
fn event_write_due(last: Instant, now: Instant) -> bool {
    now.duration_since(last) >= LAST_EVENT_WRITE_INTERVAL
}

/// Снимает чекпоинт дропнутого слота: после удаления источника строка
/// осиротела, а повторная регистрация того же имени не должна наследовать
/// чужую координату старта. Отсутствие строки — не ошибка (дроп уже был).
pub(crate) async fn delete_checkpoint(audit: &PgPool, slot: &str) -> Result<()> {
    sqlx::query("DELETE FROM checkpoints WHERE name = $1")
        .bind(slot)
        .execute(audit)
        .await?;
    Ok(())
}

/// Снимает строку источника с реестра — исполнение удаления завершено:
/// слот и публикация у клиента дропнуты. Строки журнала `changes` не
/// трогаются: журнал переживает источник. `false` — строки уже нет
/// (снята в обход DELETE или удаление исполняется повторно) — результат
/// тот же: источника в реестре больше нет.
pub(crate) async fn delete_row(audit: &PgPool, name: &str) -> Result<bool> {
    let removed = sqlx::query("DELETE FROM sources WHERE name = $1")
        .bind(name)
        .execute(audit)
        .await?;
    Ok(removed.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str) -> SourceRecord {
        SourceRecord {
            name: name.to_owned(),
            url: "postgresql://u:p@h:5432/db".to_owned(),
            ignore_tables: vec!["public.schema_migrations".to_owned()],
            message_prefix: "_audit".to_owned(),
            batch_size: 500,
            deleted_at: None,
        }
    }

    #[test]
    fn slot_and_publication_derive_from_name() {
        let record = record("client_a");
        assert_eq!(record.slot(), "waltrail_client_a");
        assert_eq!(record.publication(), "audit_client_a");
        // Слот — имя чекпоинта: чекпоинты источников не пересекаются.
        assert_eq!(record.slot(), record.slot());
    }

    #[test]
    fn settings_differ_detects_registry_edits_only() {
        let base = record("client_a");

        let mut other = base.clone();
        assert!(!base.settings_differ(&other), "копия — те же настройки");

        other.ignore_tables.push("public.extra".to_owned());
        assert!(base.settings_differ(&other), "игнор-таблицы — настройка");

        let mut other = base.clone();
        other.message_prefix = "_ctx".to_owned();
        assert!(base.settings_differ(&other), "префикс MESSAGE — настройка");

        let mut other = base.clone();
        other.batch_size = 100;
        assert!(base.settings_differ(&other), "размер батча — настройка");

        // Не настройки: URL неизменяем, статус воркера — не настройка.
        let mut other = base.clone();
        other.url = "postgresql://u:p@other/db".to_owned();
        assert!(!base.settings_differ(&other));
        let mut other = base.clone();
        other.deleted_at = Some(Utc::now());
        assert!(!base.settings_differ(&other));
    }

    #[test]
    fn event_write_throttles_within_interval() {
        let now = Instant::now();
        let earlier = now
            .checked_sub(LAST_EVENT_WRITE_INTERVAL)
            .expect("инстант в прошлом существует");
        assert!(event_write_due(earlier, now), "интервал истёк — пора");

        let recent = now
            .checked_sub(Duration::from_millis(100))
            .expect("инстант в прошлом существует");
        assert!(
            !event_write_due(recent, now),
            "внутри интервала запись не нужна"
        );
    }
}
