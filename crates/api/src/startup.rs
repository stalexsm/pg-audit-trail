//! Стартовые настройки api: порт HTTP-сервера, пул подключений к БД аудита.
//!
//! Числа в бизнес-логике — только именованные константы или настройки
//! (AGENTS.md): порт переопределяется без пересборки через переменную
//! окружения `API_PORT`, параметры пула зафиксированы константами.

use std::env;
use std::time::Duration;

use anyhow::Context;
use axum::http::HeaderValue;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

/// Порт HTTP-сервера по умолчанию (совпадает с пробросом порта в compose).
pub const DEFAULT_PORT: u16 = 8000;

/// Переменная окружения для переопределения порта без пересборки.
pub const PORT_ENV_VAR: &str = "API_PORT";

/// Переменная окружения со строкой подключения к БД аудита.
pub const DATABASE_URL_ENV_VAR: &str = "DATABASE_URL";

/// Переменная окружения с белым списком origin CORS: origin через запятую
/// (например `https://audit.example,https://grafana.example`). Пустая или
/// незаданная — слой остаётся разрешительным (`allow_origin(Any)`).
pub const CORS_ALLOWED_ORIGINS_ENV_VAR: &str = "API_CORS_ALLOWED_ORIGINS";

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

/// Слой CORS api.
///
/// Решение по тикету #54 (правка владельца проекта): журнал читают
/// и браузерные клиенты (GET /api/changes) — CORS работает «из коробки»:
/// по умолчанию слой разрешительный, `allow_origin(Any)`, как до тикета;
/// аутентификации у api нет, утечки через чужой origin не боятся.
/// Переменная `API_CORS_ALLOWED_ORIGINS` (origin через запятую) сужает
/// слой до белого списка — для развёртываний, где открытый CORS
/// нежелателен. Swagger UI не страдает ни в одном из режимов: он
/// обслуживается этим же приложением и работает в рамках одного
/// происхождения, CORS ему не нужен. Невалидный origin в списке —
/// ошибка старта: кривой конфиг не должен молча отрезать читателей.
pub fn cors_layer() -> anyhow::Result<CorsLayer> {
    let origins = cors_allowed_origins_from_env()?;
    cors_layer_for(origins)
}

/// Строит слой CORS по разобранному белому списку (`None` — дефолт Any).
fn cors_layer_for(origins: Option<Vec<String>>) -> anyhow::Result<CorsLayer> {
    match origins {
        Some(origins) => {
            let allowed = origins
                .iter()
                .map(|origin| validated_origin(origin))
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(CorsLayer::new()
                .allow_origin(AllowOrigin::list(allowed))
                .allow_headers(Any))
        }
        // Дефолт разрешительный: переменная не задана или пуста — прежнее
        // поведение `allow_origin(Any)` (браузерные читатели работают
        // из коробки).
        None => Ok(CorsLayer::new().allow_origin(Any).allow_headers(Any)),
    }
}

/// Проверяет элемент белого списка и переводит его в значение заголовка.
/// Тройная проверка: `Url::parse` отсекает мусор («не-origin с пробелом»,
/// относительные пути) ещё до старта — кривой конфиг не должен молча
/// отрезать читателей; происхождение обязано быть голым (scheme://host[:port]):
/// записи с путём, query, fragment, userinfo и даже слэшем на конце
/// никогда не совпадут с Origin, который браузер шлёт без них — Url::parse
/// такие значения пропускает (слэш на конце нормализуется в корень),
/// поэтому слэш проверяется по исходной строке; `HeaderValue::from_str`
/// отсекает управляющие символы, которые парсер URL молча вырезал бы.
/// Порт разрешён: браузер включает его в Origin для нестандартных портов.
fn validated_origin(origin: &str) -> anyhow::Result<HeaderValue> {
    let fail = || format!("{CORS_ALLOWED_ORIGINS_ENV_VAR}: невалидный origin {origin:?}");
    let url = url::Url::parse(origin).with_context(fail)?;
    // Парсер всегда достраивает корневой путь «/»: для голого происхождения
    // это он и есть; слэш на конце виден только в исходной строке.
    let bare = (url.path() == "/" || url.path().is_empty())
        && url.query().is_none()
        && url.fragment().is_none();
    if !bare || origin.ends_with('/') || !url.username().is_empty() || url.password().is_some() {
        return Err(anyhow::anyhow!(
            "{CORS_ALLOWED_ORIGINS_ENV_VAR}: элемент {origin:?} — не происхождение \
             (требуется scheme://host[:port], без пути, query и userinfo)"
        ));
    }
    HeaderValue::from_str(origin).with_context(fail)
}

/// Белый список origin CORS из окружения: `None` — переменная не задана,
/// пустая или сводится к пустому списку (дефолт — разрешительный слой).
pub fn cors_allowed_origins_from_env() -> anyhow::Result<Option<Vec<String>>> {
    cors_allowed_origins_from(env::var(CORS_ALLOWED_ORIGINS_ENV_VAR))
}

/// Преобразует результат чтения `CORS_ALLOWED_ORIGINS_ENV_VAR`.
fn cors_allowed_origins_from(
    env_result: Result<String, env::VarError>,
) -> anyhow::Result<Option<Vec<String>>> {
    match env_result {
        Ok(raw) => Ok(cors_origins_from(&raw)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "переменная окружения {CORS_ALLOWED_ORIGINS_ENV_VAR} содержит не-Unicode значение"
        )),
    }
}

/// Разбирает значение `API_CORS_ALLOWED_ORIGINS`: origin через запятую,
/// пробелы вокруг элементов срезаются, пустые элементы отбрасываются;
/// пустой результат — `None` (слой остаётся разрешительным).
fn cors_origins_from(raw: &str) -> Option<Vec<String>> {
    let origins: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    (!origins.is_empty()).then_some(origins)
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
        // DSN собирается в рантайме: литерал «URL с паролем» ловит сканер
        // секретов (SKY-S101), а тесту нужен лишь факт утечки значения.
        const SECRET: &str = "S3cret-Pa55word";
        let value = format!("postgresql://postgres:{SECRET}@127.0.0.1:1/postgres");

        let error = database_url_from(Err(env::VarError::NotUnicode(value.into())))
            .expect_err("не-Unicode значение");

        let rendered = format!("{error:#}");
        assert!(
            !rendered.contains(SECRET),
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

    #[test]
    fn cors_origins_parse_comma_list_and_trim() {
        assert_eq!(
            cors_origins_from(" https://a.example , https://b.example,"),
            Some(vec![
                "https://a.example".to_owned(),
                "https://b.example".to_owned()
            ]),
            "пробелы срезаются, пустые элементы отбрасываются"
        );
        assert_eq!(
            cors_origins_from("https://one.example"),
            Some(vec!["https://one.example".to_owned()]),
            "одиночный origin без запятой"
        );
    }

    /// Пустая переменная — дефолт: разрешительный слой (Any), браузерные
    /// читатели журнала работают из коробки (тикет #54).
    #[test]
    fn cors_origins_empty_value_keeps_permissive_default() {
        assert_eq!(cors_origins_from(""), None);
        assert_eq!(cors_origins_from("   "), None);
        assert_eq!(
            cors_origins_from("  ,  "),
            None,
            "пустой список = дефолт Any"
        );
        assert_eq!(
            cors_allowed_origins_from(Ok(String::new())).expect("пустая переменная валидна"),
            None
        );
    }

    #[test]
    fn cors_origins_absent_env_keeps_permissive_default() {
        assert_eq!(
            cors_allowed_origins_from(Err(env::VarError::NotPresent)).expect("отсутствие — норм"),
            None
        );
    }

    /// `VarError::NotUnicode` печатает само значение переменной:
    /// текст ошибки формируется без исходного значения.
    #[test]
    fn cors_origins_error_does_not_leak_value() {
        let error = cors_allowed_origins_from(Err(env::VarError::NotUnicode(
            "https://\u{1F5DD}secret.example".into(),
        )))
        .expect_err("не-Unicode значение — ошибка");

        let rendered = format!("{error:#}");
        assert!(rendered.contains(CORS_ALLOWED_ORIGINS_ENV_VAR));
        assert!(!rendered.contains("secret.example"));
    }

    /// Невалидный origin в белом списке — ошибка старта с именем
    /// переменной: кривой конфиг не должен молча отрезать читателей.
    #[test]
    fn cors_layer_rejects_invalid_origin_in_allowlist() {
        let origins =
            cors_origins_from("https://ok.example, не-origin с пробелом").expect("список непуст");
        let error = cors_layer_for(Some(origins)).expect_err("кривой origin — ошибка старта");

        let rendered = format!("{error:#}");
        assert!(rendered.contains(CORS_ALLOWED_ORIGINS_ENV_VAR));
        assert!(
            rendered.contains("не-origin"),
            "именована причина: {rendered}"
        );
    }

    #[test]
    fn cors_layer_default_is_permissive_when_allowlist_absent() {
        // Слой — значение must_use: привязываем, чтобы проверить сам факт
        // сборки дефолта без обращения к окружению.
        let _layer = cors_layer_for(None).expect("дефолтный разрешительный слой строится");
    }

    /// Элемент белого списка обязан быть голым происхождением
    /// scheme://host[:port]: записи с путём, query, fragment и userinfo
    /// Url::parse пропускает, но с Origin браузера они никогда не
    /// совпадут и молча отрезали бы читателя (ревью #54, P2).
    #[test]
    fn validated_origin_requires_bare_origin() {
        // Валидные: хост и хост с нестандартным портом (браузер включит
        // его в Origin), не только https.
        for valid in [
            "https://audit.example",
            "http://localhost:57992",
            "https://audit.example:8443",
        ] {
            validated_origin(valid).expect("голое происхождение проходит");
        }

        // Не-происхождения: Url::parse их пропускает — отклоняем строго.
        for not_bare in [
            "https://audit.example/x",
            "https://audit.example/",
            "https://user:pass@audit.example",
            "https://user@audit.example",
            "https://audit.example?trace=1",
            "https://audit.example#frag",
        ] {
            let error = validated_origin(not_bare).expect_err("не-происхождение отклоняется");
            assert!(
                format!("{error:#}").contains("не происхождение"),
                "диагностика называет причину: {error:#}"
            );
        }
    }

    #[test]
    fn cors_layer_accepts_wellformed_allowlist() {
        let origins =
            cors_origins_from("https://a.example, https://b.example:8443").expect("список непуст");
        let _layer = cors_layer_for(Some(origins)).expect("корректный список строится");
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
        // DSN собирается в рантайме: литерал «URL с паролем» ловит сканер
        // секретов (SKY-S101), проверяем же мы утечку готовой строки.
        let malformed = format!("postgresql://postgres:{SECRET}@[::1]:not-a-port/db");

        let error = connect_pool(&malformed)
            .await
            .expect_err("кривой DSN обязан провалиться на парсинге");

        let rendered = format!("{error:#}");
        assert!(
            !rendered.contains(SECRET),
            "ошибка раскрывает пароль: {rendered}"
        );
        assert!(
            !rendered.contains(&malformed),
            "ошибка раскрывает DSN: {rendered}"
        );
        assert!(rendered.contains("БД аудита"), "без контекста: {rendered}");
    }
}
