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

impl WorkerConfig {
    /// Читает конфигурацию из окружения: без префикса — URL баз,
    /// с префиксом `CDC_` — настройки воркера.
    pub fn from_env() -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(Environment::default())
            .add_source(
                Environment::with_prefix("CDC")
                    .try_parsing(true)
                    .list_separator(","),
            )
            .build()?;

        Ok(settings.try_deserialize()?)
    }
}
