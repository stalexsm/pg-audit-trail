//! Общая пауза повторов: экспоненциальный backoff реконнекта, прерываемый
//! остановкой. Используется задачей источника (реконнект репликации) и
//! исполнением удаления источника (повторы дропа слота и публикации).

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
pub(crate) async fn pause(
    token: &CancellationToken,
    supervisor_gone: &mut watch::Receiver<()>,
    delay: Duration,
) -> bool {
    tokio::select! {
        _ = token.cancelled() => false,
        _ = supervisor_gone.changed() => false,
        _ = tokio::time::sleep(delay) => true,
    }
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

    #[test]
    fn reconnect_delay_reads_worker_settings() {
        let config = WorkerConfig {
            database_url: "postgresql://audit/db".to_owned(),
            reconnect_initial_backoff_ms: 250,
            reconnect_max_backoff_ms: 45_000,
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
