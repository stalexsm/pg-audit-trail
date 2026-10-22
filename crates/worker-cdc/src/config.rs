use config::Environment;
use serde::Deserialize;

use crate::Result;

/// Таймаут взятия соединения из пулов БД, секунды (общий для супервизора
/// и задач источников).
pub(crate) const DB_ACQUIRE_TIMEOUT_SECS: u64 = 30;

/// Таймаут подключения задачи источника к БД клиента, секунды: создание
/// пула попытки ограничено снаружи — зависший DNS/TCP не держит попытку
/// дольше бюджета, задача уходит в видимый `error` с backoff.
pub(crate) const SOURCE_CONNECT_TIMEOUT_SECS: u64 = 30;

/// Таймаут бутстрапа источника, секунды: суммарный бюджет всех запросов
/// бутстрапа (каталог, публикация, слот, чекпоинт).
pub(crate) const BOOTSTRAP_TIMEOUT_SECS: u64 = 30;

/// Таймаут подтверждения старта репликации (`wait_started`), секунды.
/// Подключение walsender фоновое: TCP, аутентификация и START_REPLICATION
/// завершаются не позже подтверждения старта, поэтому таймаут на нём
/// ограничивает весь тракт подключения; без него зависание сети оставляло
/// бы задачу в статусе `starting` навсегда.
pub(crate) const REPLICATION_START_TIMEOUT_SECS: u64 = 15;

/// Таймаут штатного закрытия репликации при остановке, секунды: SIGTERM
/// обязан уложиться в grace-окно оркестратора даже на зависшей сети —
/// по истечении таймаута соединение бросается на дроп клиента.
pub(crate) const REPLICATION_SHUTDOWN_TIMEOUT_SECS: u64 = 5;

/// Глобальная конфигурация воркера: собирается из окружения поверх дефолтов
/// (крейт `config`). Настройки источников — строки подключения, служебные
/// таблицы, префикс MESSAGE, размер батча — из окружения не читаются:
/// их единственный источник правды — реестр источников в БД аудита (ADR-0005).
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    /// Подключение к БД аудита (`DATABASE_URL`): журнал, реестр источников,
    /// чекпоинты.
    pub database_url: String,

    /// Начальная пауза реконнекта задачи источника после обрыва соединения,
    /// в миллисекундах (`CDC_RECONNECT_INITIAL_BACKOFF_MS`).
    #[serde(default = "default_reconnect_initial_backoff_ms")]
    pub reconnect_initial_backoff_ms: u64,

    /// Верхняя граница экспоненциального backoff реконнекта,
    /// в миллисекундах (`CDC_RECONNECT_MAX_BACKOFF_MS`).
    #[serde(default = "default_reconnect_max_backoff_ms")]
    pub reconnect_max_backoff_ms: u64,
}

fn default_reconnect_initial_backoff_ms() -> u64 {
    1_000
}

fn default_reconnect_max_backoff_ms() -> u64 {
    30_000
}

impl WorkerConfig {
    /// Читает конфигурацию из окружения: без префикса — URL БД аудита,
    /// с префиксом `CDC_` — глобальные настройки воркера. Источниковые
    /// настройки из окружения не читаются вовсе (ADR-0005).
    pub fn from_env() -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(Environment::default())
            .add_source(
                Environment::with_prefix("CDC")
                    // Числовые значения CDC_* приходят строками: без
                    // try_parsing «250» не приводится к u64 и поле молча
                    // остаётся на дефолте.
                    .try_parsing(true),
            )
            .build()?;

        Ok(settings.try_deserialize()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Из окружения читаются только глобальные настройки; переехавшие в
    /// реестр переменные (`SOURCE_DATABASE_URL`, `CDC_SLOT`, …) воркером
    /// больше не читаются — их наличие не влияет на конфигурацию.
    #[test]
    fn env_reads_only_global_settings() {
        // Глобальное состояние окружения: правка безопасна — других
        // env-тестов в этом бинарнике нет; значения возвращаются в конце.
        unsafe {
            std::env::set_var("DATABASE_URL", "postgresql://audit/db");
            std::env::set_var("CDC_RECONNECT_INITIAL_BACKOFF_MS", "250");
            std::env::set_var("CDC_RECONNECT_MAX_BACKOFF_MS", "45000");
            std::env::set_var("SOURCE_DATABASE_URL", "postgresql://src/db");
            std::env::set_var("CDC_SLOT", "legacy-slot");
            std::env::set_var("CDC_PUBLICATION", "legacy-pub");
            std::env::set_var("CDC_MESSAGE_PREFIX", "_legacy");
            std::env::set_var("CDC_IGNORE_TABLES", "public.a,public.b");
            std::env::set_var("CDC_BATCH_SIZE", "42");
        }

        let config = WorkerConfig::from_env().expect("конфиг из окружения");
        assert_eq!(config.database_url, "postgresql://audit/db");
        assert_eq!(config.reconnect_initial_backoff_ms, 250);
        assert_eq!(config.reconnect_max_backoff_ms, 45_000);

        unsafe {
            std::env::remove_var("DATABASE_URL");
            std::env::remove_var("CDC_RECONNECT_INITIAL_BACKOFF_MS");
            std::env::remove_var("CDC_RECONNECT_MAX_BACKOFF_MS");
            std::env::remove_var("SOURCE_DATABASE_URL");
            std::env::remove_var("CDC_SLOT");
            std::env::remove_var("CDC_PUBLICATION");
            std::env::remove_var("CDC_MESSAGE_PREFIX");
            std::env::remove_var("CDC_IGNORE_TABLES");
            std::env::remove_var("CDC_BATCH_SIZE");
        }
    }

    /// Без переопределений — дефолты реконнекта.
    #[test]
    fn reconnect_defaults() {
        let config = WorkerConfig {
            database_url: "postgresql://audit/db".to_owned(),
            reconnect_initial_backoff_ms: default_reconnect_initial_backoff_ms(),
            reconnect_max_backoff_ms: default_reconnect_max_backoff_ms(),
        };
        assert_eq!(config.reconnect_initial_backoff_ms, 1_000);
        assert_eq!(config.reconnect_max_backoff_ms, 30_000);
    }
}
