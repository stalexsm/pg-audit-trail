//! Общая обвязка логирования бинарей workspace (api и worker-cdc).
//!
//! Единая инициализация `tracing-subscriber`: фильтр по `RUST_LOG`
//! (по умолчанию `info`), файловый слой с ротацией по дням (без ANSI,
//! рядом с бинарником или в корне workspace в режиме разработки) и слой
//! в терминал (с ANSI). Бинарям остаётся вызвать [`init`] с именем своего
//! файла лога и держать возвращённый guard до конца работы процесса.

use std::env;
use std::path::PathBuf;

use anyhow::Context;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Псевдоним ошибок крейта по конвенции репозитория (AGENTS.md).
pub(crate) type Result<T, E = anyhow::Error> = core::result::Result<T, E>;

/// Каталог файлов логов относительно базового каталога.
const LOG_DIR: &str = "logs";

/// Фильтр по умолчанию, если `RUST_LOG` не задан.
const DEFAULT_ENV_FILTER: &str = "info";

/// Инициализирует глобальный сборщик событий `tracing` для бинаря.
///
/// `log_file_name` — имя файла лога бинаря (например, `api.log`);
/// ротация — по дням, каталог `logs` внутри [`base_dir`].
/// Фильтр берётся из `RUST_LOG`, без переменной — `DEFAULT_ENV_FILTER`.
///
/// Возвращает guard неблокирующего писателя: его нужно держать до конца
/// работы процесса, иначе хвост логов не будет сброшен в файл.
pub fn init(log_file_name: &str) -> Result<WorkerGuard> {
    let (non_blocking, guard) =
        tracing_appender::non_blocking(rolling::daily(base_dir()?.join(LOG_DIR), log_file_name));
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_ENV_FILTER));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_level(true)
                .with_target(false),
        )
        .with(
            fmt::layer()
                .with_ansi(true)
                .with_level(true)
                .with_target(false),
        )
        .init();

    Ok(guard)
}

/// Базовый каталог для файлов логов: в режиме разработки — корень workspace,
/// в проде — каталог исполняемого файла.
fn base_dir() -> Result<PathBuf> {
    if cfg!(debug_assertions) {
        return PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .map(PathBuf::from)
            .context("корень workspace не найден от каталога крейта");
    }

    let exe = env::current_exe().context("каталог исполняемого файла")?;
    Ok(exe.parent().map(PathBuf::from).unwrap_or(exe))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// В тестах (debug-сборка) базовый каталог — корень workspace: логи
    /// не должны расползаться по вложенным каталогам крейтов.
    #[test]
    fn base_dir_in_debug_is_workspace_root() {
        let dir = base_dir().expect("корень workspace");

        assert!(dir.is_dir(), "корень workspace не существует: {dir:?}");
        assert!(
            dir.join("Cargo.toml").is_file(),
            "в корне workspace нет Cargo.toml: {dir:?}"
        );
    }

    /// Дефолтный фильтр разбирается `EnvFilter` (он паникует на некорректной
    /// директиве): отсутствие `RUST_LOG` не должно ломать запуск бинаря.
    #[test]
    fn default_filter_is_parseable() {
        EnvFilter::new(DEFAULT_ENV_FILTER);
    }
}
