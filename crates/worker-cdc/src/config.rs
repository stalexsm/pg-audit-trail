use config::Environment;
use serde::Deserialize;

use crate::Result;

/// Конфигурация воркера CDC: собирается из окружения поверх дефолтов
/// (крейт `config`; URL баз — без префикса, настройки воркера — `CDC_*`).
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    /// Подключение к БД аудита (`DATABASE_URL`).
    pub database_url: String,

    /// Подключение к БД-источнику CDC (`SOURCE_DATABASE_URL`).
    pub source_database_url: String,

    /// Имя слота логической репликации (`CDC_SLOT`).
    #[serde(default = "default_slot")]
    pub slot: String,

    /// Имя публикации на источнике (`CDC_PUBLICATION`).
    #[serde(default = "default_publication")]
    pub publication: String,

    /// Игнорируемые (служебные) таблицы источника в виде `schema.table`
    /// (`CDC_IGNORE_TABLES`, через запятую).
    #[serde(default = "default_ignore_tables")]
    pub ignore_tables: Vec<String>,

    /// Размер чанка батчевой вставки строк в журнал (`CDC_BATCH_SIZE`).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Начальная пауза реконнекта после обрыва соединения с источником,
    /// в миллисекундах (`CDC_RECONNECT_INITIAL_BACKOFF_MS`).
    #[serde(default = "default_reconnect_initial_backoff_ms")]
    pub reconnect_initial_backoff_ms: u64,

    /// Верхняя граница экспоненциального backoff реконнекта,
    /// в миллисекундах (`CDC_RECONNECT_MAX_BACKOFF_MS`).
    #[serde(default = "default_reconnect_max_backoff_ms")]
    pub reconnect_max_backoff_ms: u64,
}

fn default_slot() -> String {
    "waltrail".to_owned()
}

fn default_publication() -> String {
    "audit".to_owned()
}

/// Служебные таблицы типовых миграторов: в журнал они не попадают.
fn default_ignore_tables() -> Vec<String> {
    vec![
        "public.alembic_version".to_owned(),
        "public.schema_migrations".to_owned(),
        "public._sqlx_migrations".to_owned(),
        "public.django_migrations".to_owned(),
    ]
}

fn default_batch_size() -> usize {
    500
}

fn default_reconnect_initial_backoff_ms() -> u64 {
    1_000
}

fn default_reconnect_max_backoff_ms() -> u64 {
    30_000
}

impl WorkerConfig {
    /// Читает конфигурацию из окружения: без префикса — URL баз,
    /// с префиксом `CDC_` — настройки воркера.
    pub fn from_env() -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(Environment::default())
            .add_source(
                Environment::with_prefix("CDC")
                    .try_parsing(true)
                    // Без with_list_parse_key список-парсинг превращает в
                    // Vec<String> все CDC_* значения, ломая строковые поля.
                    .list_separator(",")
                    .with_list_parse_key("ignore_tables"),
            )
            .build()?;

        Ok(settings.try_deserialize()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CDC_* строки приходят строками, а не списками: список — только
    /// ignore_tables. Регрессия config-rs: list_separator + try_parsing
    /// без with_list_parse_key превращает все CDC_* значения в Vec<String>,
    /// и любой заданный оператором CDC_SLOT валил воркер.
    #[test]
    fn env_strings_stay_strings_and_only_ignore_tables_is_a_list() {
        // Глобальное состояние окружения: правка безопасна — других
        // env-тестов в этом бинарнике нет.
        unsafe {
            std::env::set_var("DATABASE_URL", "postgresql://audit/db");
            std::env::set_var("SOURCE_DATABASE_URL", "postgresql://src/db");
            std::env::set_var("CDC_SLOT", "survival-slot");
            std::env::set_var("CDC_IGNORE_TABLES", "public.a,public.b");
        }

        let config = WorkerConfig::from_env().expect("конфиг из окружения");
        assert_eq!(config.slot, "survival-slot");
        assert_eq!(config.publication, "audit");
        assert_eq!(
            config.ignore_tables,
            vec!["public.a".to_owned(), "public.b".to_owned()]
        );
    }
}
