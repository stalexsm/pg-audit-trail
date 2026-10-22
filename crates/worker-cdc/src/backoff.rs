//! Общая пауза повторов: экспоненциальный backoff реконнекта с джиттером,
//! прерываемый остановкой. Используется задачей источника (реконнект
//! репликации) и исполнением удаления источника (повторы дропа слота
//! и публикации).

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::WorkerConfig;

/// Потолок степени экспоненты backoff: 2^30 миллисекунд — далеко за любым
/// разумным верхом, защита от переполнения умножения.
const MAX_BACKOFF_SHIFT: u32 = 30;

/// Задержка перед (пере)подключением по номеру подряд идущей попытки:
/// глобальные настройки реконнекта воркера.
pub(crate) fn reconnect_delay(config: &WorkerConfig, attempt: u32) -> Duration {
    exponential(
        Duration::from_millis(config.reconnect_initial_backoff_ms),
        Duration::from_millis(config.reconnect_max_backoff_ms),
        attempt,
    )
}

/// Экспоненциальный backoff `initial * 2^attempt`, ограниченный сверху `max`;
/// ниже `initial` не опускается даже при `max < initial`.
pub(crate) fn exponential(initial: Duration, max: Duration, attempt: u32) -> Duration {
    // Потолок степени защищает от переполнения при больших номерах попыток.
    let doubling = initial.saturating_mul(2u32.saturating_pow(attempt.min(MAX_BACKOFF_SHIFT)));
    doubling.min(max).max(initial)
}

/// Пауза перед повтором, прерываемая остановкой (токен воркера или
/// исчезновение супервизора): `false` — остановка пришла в паузе.
///
/// Пауза получает джиттер (`jittered`): одновременный сбой нескольких
/// источников не выстраивает их реконнекты в общий такт — повторы не
/// синхронизируются и не долбят БД аудита одновременными волнами.
pub(crate) async fn pause(
    token: &CancellationToken,
    supervisor_gone: &mut watch::Receiver<()>,
    delay: Duration,
) -> bool {
    let delay = jittered(delay);
    tokio::select! {
        _ = token.cancelled() => false,
        _ = supervisor_gone.changed() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// Джиттер паузы повтора: случайная доля верхней половины паузы
/// (`delay/2 … delay`).
///
/// Равный джиттер (а не полный `0 … delay`) сохраняет нижний предел
/// `delay/2`: нулевая пауза реконнекта долбила бы БД в такт сбоям.
/// Случайность — без внешних зависимостей: ключи `RandomState` сидируются
/// из ОС и меняются при каждом вызове, хэш пустого ввода даёт независимые
/// 64 бита на вызов (тикет #54).
pub(crate) fn jittered(delay: Duration) -> Duration {
    let millis = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
    let half = millis / 2;
    // Взятие остатка вместо умножения на дробь: u64-деление без float
    // и без переполнения; +1 делает возможным и полный `delay`.
    let extra = if half == 0 {
        0
    } else {
        random_u64() % (half + 1)
    };
    Duration::from_millis(half + extra)
}

/// Случайные 64 бита: хэш пустого ввода свежим `RandomState`.
fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_starts_at_initial_and_doubles() {
        let initial = Duration::from_millis(200);
        let max = Duration::from_secs(30);
        assert_eq!(exponential(initial, max, 0), initial);
        assert_eq!(exponential(initial, max, 1), Duration::from_millis(400));
        assert_eq!(exponential(initial, max, 2), Duration::from_millis(800));
        assert_eq!(exponential(initial, max, 3), Duration::from_millis(1600));
    }

    #[test]
    fn backoff_is_capped_at_max() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(30);
        assert_eq!(exponential(initial, max, 5), max);
        assert_eq!(
            exponential(initial, max, 100),
            max,
            "переполнение степени не должно пробить верхнюю границу"
        );
    }

    #[test]
    fn backoff_floor_is_initial_even_when_max_is_smaller() {
        let initial = Duration::from_secs(5);
        assert_eq!(
            exponential(initial, Duration::from_secs(1), 3),
            initial,
            "нулевая пауза реконнекта недопустима"
        );
    }

    /// Джиттер держит паузу в верхней половине: ниже `delay/2` не падает
    /// (нулевой реконнект недопустим), выше `delay` не поднимается.
    #[test]
    fn jitter_keeps_delay_in_upper_half() {
        let base = Duration::from_millis(1_000);
        for _ in 0..100 {
            let delay = jittered(base);
            assert!(
                delay >= Duration::from_millis(500),
                "ниже половины: {delay:?}"
            );
            assert!(delay <= base, "выше исходной паузы: {delay:?}");
        }
    }

    /// Соседние вызовы дают разные паузы: иначе джиттер не развязывал бы
    /// повторы источников. Событие «все 100 выборок совпали» при 64
    /// случайных битах на вызов практически исключено.
    #[test]
    fn jitter_varies_between_calls() {
        let base = Duration::from_millis(10_000);
        let sample: std::collections::HashSet<Duration> =
            (0..100).map(|_| jittered(base)).collect();
        assert!(
            sample.len() > 1,
            "джиттер вырождается в константу: {sample:?}"
        );
    }

    #[test]
    fn jitter_of_zero_and_tiny_delays_stays_bounded() {
        assert_eq!(jittered(Duration::ZERO), Duration::ZERO);
        let tiny = jittered(Duration::from_millis(1));
        assert!(tiny <= Duration::from_millis(1));
    }

    #[test]
    fn reconnect_delay_reads_worker_settings() {
        let config = WorkerConfig {
            database_url: "postgresql://audit/db".to_owned(),
            reconnect_initial_backoff_ms: 250,
            reconnect_max_backoff_ms: 45_000,
            tx_buffer_max_rows: 100_000,
        };
        assert_eq!(
            reconnect_delay(&config, 0),
            Duration::from_millis(250),
            "первая пауза — начальная"
        );
        assert_eq!(
            reconnect_delay(&config, 20),
            Duration::from_millis(45_000),
            "дальние паузы — под верхней границей"
        );
    }
}
