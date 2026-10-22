//! Стартовые настройки api: порт HTTP-сервера, пул подключений к БД аудита.
//!
//! Числа в бизнес-логике — только именованные константы или настройки
//! (AGENTS.md): порт переопределяется без пересборки через переменную
//! окружения `API_PORT`, параметры пула зафиксированы константами.

use std::env;
use std::time::Duration;

use anyhow::Context;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Порт HTTP-сервера по умолчанию (совпадает с пробросом порта в compose).
pub const DEFAULT_PORT: u16 = 8000;

/// Переменная окружения для переопределения порта без пересборки.
pub const PORT_ENV_VAR: &str = "API_PORT";

/// Переменная окружения со строкой подключения к БД аудита.
pub const DATABASE_URL_ENV_VAR: &str = "DATABASE_URL";

/// Размер пула подключений к БД аудита.
pub const POOL_MAX_CONNECTIONS: u32 = 20;

/// Таймаут взятия соединения из пула, секунды.
pub const POOL_ACQUIRE_TIMEOUT_SECS: u64 = 3;

/// Разбирает значение `API_PORT` в номер порта.
pub fn parse_port(raw: &str) -> anyhow::Result<u16> {
    raw.trim()
        .parse::<u16>()
        .with_context(|| format!("{PORT_ENV_VAR}: ожидался номер порта, получено {raw:?}"))
}

/// Порт HTTP-сервера: `API_PORT` из окружения или `DEFAULT_PORT`.
pub fn port_from_env() -> anyhow::Result<u16> {
    match env::var(PORT_ENV_VAR) {
        Ok(raw) => parse_port(&raw),
        Err(env::VarError::NotPresent) => Ok(DEFAULT_PORT),
        Err(err) => Err(err).context(format!("переменная окружения {PORT_ENV_VAR}")),
    }
}

/// Строка подключения к БД аудита из окружения.
///
/// Текст ошибки не включает значение переменной: `VarError::NotUnicode`
/// печатает само значение — вместе с ним в логи контейнера ушёл бы пароль
/// из DSN, поэтому сообщение формируется без исходной ошибки.
pub fn database_url_from_env() -> anyhow::Result<String> {
    database_url_from(env::var(DATABASE_URL_ENV_VAR))
}

/// Преобразует результат чтения `DATABASE_URL_ENV_VAR` из окружения.
fn database_url_from(env_result: Result<String, env::VarError>) -> anyhow::Result<String> {
    match env_result {
        Ok(url) => Ok(url),
        Err(env::VarError::NotPresent) => Err(anyhow::anyhow!(
            "переменная окружения {DATABASE_URL_ENV_VAR} не задана"
        )),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "переменная окружения {DATABASE_URL_ENV_VAR} содержит не-Unicode значение"
        )),
    }
}

/// Единая точка конфигурации пула подключений к БД аудита.
pub fn pool_options() -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(POOL_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(POOL_ACQUIRE_TIMEOUT_SECS))
}

/// Подключение к БД аудита.
///
/// Контракт безопасности: строка подключения (пароль DSN) не попадает
/// в текст ошибки и логи контейнера при неудачном старте — контекст
/// ошибки не включает url, sqlx текст соединения не раскрывает.
pub async fn connect_pool(database_url: &str) -> anyhow::Result<PgPool> {
    pool_options()
        .connect(database_url)
        .await
        .context("не удалось подключиться к БД аудита (DATABASE_URL)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_port() {
        assert_eq!(parse_port("8080").expect("корректный порт"), 8080);
    }

    #[test]
    fn parses_port_with_surrounding_spaces() {
        assert_eq!(parse_port(" 9090 ").expect("корректный порт"), 9090);
    }

    #[test]
    fn rejects_non_numeric_port() {
        let error = parse_port("восемьдесят").expect_err("некорректный порт");
        // Сообщение ошибки называет переменную — понятно, что чинить.
        assert!(format!("{error:#}").contains(PORT_ENV_VAR));
    }

    #[test]
    fn rejects_out_of_range_port() {
        assert!(parse_port("70000").is_err());
    }

    #[test]
    fn default_port_matches_compose_mapping() {
        assert_eq!(DEFAULT_PORT, 8000);
    }

    #[test]
    fn database_url_passes_through() {
        let url = "postgresql://postgres:postgres@db1:5432/postgres";

        assert_eq!(
            database_url_from(Ok(url.to_owned())).expect("корректный url"),
            url
        );
    }

    #[test]
    fn database_url_missing_error_names_variable() {
        let error = database_url_from(Err(env::VarError::NotPresent)).expect_err("нет переменной");

        assert!(format!("{error:#}").contains(DATABASE_URL_ENV_VAR));
    }

    /// `VarError::NotUnicode` печатает значение переменной вместе с паролем:
    /// текст ошибки обязан его скрыть.
    #[test]
    fn database_url_error_does_not_leak_value() {
        let error = database_url_from(Err(env::VarError::NotUnicode(
            "postgresql://postgres:S3cret-Pa55word@127.0.0.1:1/postgres".into(),
        )))
        .expect_err("не-Unicode значение");

        let rendered = format!("{error:#}");
        assert!(
            !rendered.contains("S3cret"),
            "ошибка раскрывает значение: {rendered}"
        );
        assert!(
            !rendered.contains("postgres"),
            "ошибка раскрывает значение: {rendered}"
        );
        assert!(
            rendered.contains(DATABASE_URL_ENV_VAR),
            "без имени переменной: {rendered}"
        );
    }

    #[test]
    fn pool_options_applies_named_constants() {
        let options = pool_options();
        assert_eq!(options.get_max_connections(), POOL_MAX_CONNECTIONS);
        assert_eq!(
            options.get_acquire_timeout(),
            Duration::from_secs(POOL_ACQUIRE_TIMEOUT_SECS)
        );
    }

    /// Пароль из строки подключения не попадает в текст ошибки: при
    /// неудачном старте DSN не должен уйти в логи контейнера.
    /// Подключение к закрытому порту локального хоста падает быстро.
    #[tokio::test]
    async fn connect_error_does_not_leak_dsn() {
        const SECRET: &str = "S3cret-Pa55word-#51";
        let database_url = format!("postgresql://postgres:{SECRET}@127.0.0.1:1/postgres");

        let error = connect_pool(&database_url)
            .await
            .expect_err("подключение к закрытому порту обязано провалиться");

        let rendered = format!("{error:#}");
        assert!(
            !rendered.contains(SECRET),
            "текст ошибки раскрывает пароль: {rendered}"
        );
        assert!(
            !rendered.contains(&database_url),
            "текст ошибки раскрывает DSN целиком: {rendered}"
        );
        // Контекст ошибки понятен без DSN.
        assert!(rendered.contains("БД аудита"), "без контекста: {rendered}");
    }

    /// Кривой DSN (некорректный порт) падает на парсинге url — без сети:
    /// sqlx и в этом случае не включает саму строку подключения в текст
    /// ошибки, пароль не уходит в логи при таком сбое.
    #[tokio::test]
    async fn connect_error_on_malformed_dsn_does_not_leak_it() {
        const SECRET: &str = "S3cret-Pa55word-#51";
        const MALFORMED: &str = "postgresql://postgres:S3cret-Pa55word-#51@[::1]:not-a-port/db";

        let error = connect_pool(MALFORMED)
            .await
            .expect_err("кривой DSN обязан провалиться на парсинге");

        let rendered = format!("{error:#}");
        assert!(
            !rendered.contains(SECRET),
            "ошибка раскрывает пароль: {rendered}"
        );
        assert!(
            !rendered.contains(MALFORMED),
            "ошибка раскрывает DSN: {rendered}"
        );
        assert!(rendered.contains("БД аудита"), "без контекста: {rendered}");
    }
}
