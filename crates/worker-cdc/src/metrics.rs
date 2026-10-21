//! Метрики пропускной способности конвейера CDC (PoC-гейт производительности,
//! тикет #10): три ступени конвейера — walsender (байты потока репликации),
//! intake (декодирование и сборка строк журнала), вставка в `changes`.
//!
//! Счётчики дешёвые (атомики, без аллокаций на горячем пути), длительности
//! ступеней — накопительные: занятость ступени = её чистое время / время
//! стены, что на последовательном конвейере и показывает узкое место.
//! Раз в `REPORT_INTERVAL_SECS` секунд сводка пишется в лог (`info`,
//! цель `worker_cdc::metrics`), история длительностей батчей хранится
//! в ограниченном кольце для перцентилей p50/p99.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::info;

/// Период сводного отчёта метрик, секунды.
const REPORT_INTERVAL_SECS: u64 = 10;

/// Глубина истории длительностей вставки батчей: последних N батчей
/// достаточно для устойчивых p50/p99 на интервале отчёта.
const BATCH_HISTORY_CAP: usize = 4096;

const MIB: f64 = 1024.0 * 1024.0;

/// Мгновенный срез счётчиков: тоталы с старта сессии плюс копия истории
/// длительностей батчей для перцентилей.
#[derive(Debug, Default, Clone)]
struct Snapshot {
    wire_bytes: u64,
    events: u64,
    inserted_rows: u64,
    batches: u64,
    intake_nanos: u64,
    insert_nanos: u64,
    batch_ms: Vec<u64>,
}

impl Snapshot {
    fn empty() -> Self {
        Self::default()
    }
}

/// Счётчики конвейера: атомики и кольцо длительностей батчей.
#[derive(Debug, Default)]
struct Metrics {
    wire_bytes: AtomicU64,
    events: AtomicU64,
    inserted_rows: AtomicU64,
    batches: AtomicU64,
    intake_nanos: AtomicU64,
    insert_nanos: AtomicU64,
    batch_ms: Mutex<VecDeque<u64>>,
}

impl Metrics {
    fn add_wire(&self, bytes: usize) {
        self.wire_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn add_event(&self) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }

    fn add_intake_time(&self, spent: Duration) {
        self.intake_nanos
            .fetch_add(spent.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Фиксирует вставку одного батча: длительность ступени, строки и запись
    /// в историю длительностей (мс) для перцентилей.
    fn add_flush(&self, spent: Duration, rows: usize) {
        self.inserted_rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.insert_nanos
            .fetch_add(spent.as_nanos() as u64, Ordering::Relaxed);
        let ms = (spent.as_secs_f64() * 1000.0).round() as u64;
        if let Ok(mut history) = self.batch_ms.lock() {
            if history.len() == BATCH_HISTORY_CAP {
                history.pop_front();
            }
            history.push_back(ms);
        }
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            wire_bytes: self.wire_bytes.load(Ordering::Relaxed),
            events: self.events.load(Ordering::Relaxed),
            inserted_rows: self.inserted_rows.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            intake_nanos: self.intake_nanos.load(Ordering::Relaxed),
            insert_nanos: self.insert_nanos.load(Ordering::Relaxed),
            batch_ms: self
                .batch_ms
                .lock()
                .map(|history| history.iter().copied().collect())
                .unwrap_or_default(),
        }
    }
}

/// Глобальные счётчики конвейера: одна сессия репликации на процесс.
static METRICS: Metrics = Metrics {
    wire_bytes: AtomicU64::new(0),
    events: AtomicU64::new(0),
    inserted_rows: AtomicU64::new(0),
    batches: AtomicU64::new(0),
    intake_nanos: AtomicU64::new(0),
    insert_nanos: AtomicU64::new(0),
    batch_ms: Mutex::new(VecDeque::new()),
};

/// Байты потока репликации (ступень walsender): XLogData и MESSAGE payload.
pub(crate) fn add_wire(bytes: usize) {
    METRICS.add_wire(bytes);
}

/// Событие подготовлено к вставке в журнал (ступень intake).
pub(crate) fn add_event() {
    METRICS.add_event();
}

/// Чистое время ступени intake (декодирование, сборка строк, payload MESSAGE).
pub(crate) fn add_intake_time(spent: Duration) {
    METRICS.add_intake_time(spent);
}

/// Вставка батча в `changes` (ступень вставки) зафиксирована.
pub(crate) fn add_flush(spent: Duration, rows: usize) {
    METRICS.add_flush(spent, rows);
}

/// Цикл сводных отчётов: раз в `REPORT_INTERVAL_SECS` пишет в лог скорости
/// ступеней за интервал, занятость ступеней, перцентили вставки и накопленный
/// остаток строк в буферах транзакций. Завершается по `CancellationToken`.
pub(crate) async fn report_loop(token: CancellationToken) {
    let mut ticker = tokio::time::interval(Duration::from_secs(REPORT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Первый тик tokio срабатывает мгновенно — пропускаем, чтобы сводка
    // считалась по полному интервалу.
    ticker.tick().await;
    let mut previous = Snapshot::empty();
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = ticker.tick() => previous = report_interval(previous),
        }
    }
}

/// Считает и пишет сводку за интервал: возвращает срез для следующего дельта-расчёта.
fn report_interval(previous: Snapshot) -> Snapshot {
    let current = METRICS.snapshot();
    let seconds = REPORT_INTERVAL_SECS as f64;

    let wire_mib_s = current.wire_bytes.saturating_sub(previous.wire_bytes) as f64 / seconds / MIB;
    let events_ps = current.events.saturating_sub(previous.events) as f64 / seconds;
    let rows_ps = current.inserted_rows.saturating_sub(previous.inserted_rows) as f64 / seconds;

    // Занятость ступени: её чистое время за интервал к времени стены.
    let intake_pct = occupancy_percent(current.intake_nanos.saturating_sub(previous.intake_nanos));
    let insert_pct = occupancy_percent(current.insert_nanos.saturating_sub(previous.insert_nanos));

    let p50 = percentile(&current.batch_ms, 50);
    let p99 = percentile(&current.batch_ms, 99);
    let backlog = current.events as i64 - current.inserted_rows as i64;

    info!(
        "метрики CDC [за {REPORT_INTERVAL_SECS} с]: walsender {wire_mib_s:.1} МиБ/с | \
         intake {events_ps:.0} соб/с (занятость {intake_pct:.0}%) | \
         вставка {rows_ps:.0} строк/с, батчей {}, p50 {p50} мс, p99 {p99} мс \
         (занятость {insert_pct:.0}%) | всего {} строк, в буферах {backlog}",
        current.batches.saturating_sub(previous.batches),
        current.inserted_rows,
    );
    current
}

/// Перцентиль методом ближайшего ранга по возрастающему срезу; пустая
/// история — 0.
fn percentile(values: &[u64], p: u64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted: Vec<u64> = values.to_vec();
    sorted.sort_unstable();
    let rank = (p * sorted.len() as u64).div_ceil(100).max(1);
    sorted[(rank - 1).min(sorted.len() as u64 - 1) as usize]
}

/// Занятость ступени в процентах: её чистое время (наносекунды), отнесённое
/// к длительности интервала отчёта.
fn occupancy_percent(nanos: u64) -> f64 {
    const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
    let interval_nanos = REPORT_INTERVAL_SECS as f64 * NANOS_PER_SECOND;
    nanos as f64 / interval_nanos * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_handles_empty_and_single_value() {
        assert_eq!(percentile(&[], 99), 0);
        assert_eq!(percentile(&[42], 50), 42);
        assert_eq!(percentile(&[42], 99), 42);
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        let values: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&values, 50), 50);
        assert_eq!(percentile(&values, 99), 99);
        assert_eq!(percentile(&values, 100), 100);
        // Для n = 1000 p99 — 990-й по порядку.
        let values: Vec<u64> = (1..=1000).collect();
        assert_eq!(percentile(&values, 99), 990);
    }

    #[test]
    fn percentile_sorts_unsorted_input() {
        assert_eq!(percentile(&[300, 100, 200], 50), 200);
        assert_eq!(percentile(&[300, 100, 200], 99), 300);
    }

    #[test]
    fn occupancy_is_share_of_report_interval() {
        let interval_nanos = REPORT_INTERVAL_SECS * 1_000_000_000;
        assert_eq!(occupancy_percent(0), 0.0);
        assert_eq!(occupancy_percent(interval_nanos), 100.0);
        assert_eq!(occupancy_percent(interval_nanos / 4), 25.0);
        assert_eq!(
            occupancy_percent(interval_nanos * 2),
            200.0,
            "дренаж может превышать стену на границе интервалов"
        );
    }

    #[test]
    fn fresh_metrics_reports_empty_snapshot() {
        let metrics = Metrics::default();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.events, 0);
        assert!(snapshot.batch_ms.is_empty());
        assert_eq!(percentile(&snapshot.batch_ms, 99), 0);
    }

    #[test]
    fn flush_counters_accumulate_and_history_is_capped() {
        let metrics = Metrics::default();
        for i in 0..(BATCH_HISTORY_CAP + 10) {
            metrics.add_flush(Duration::from_millis(i as u64), 10);
        }
        metrics.add_wire(1024);
        metrics.add_event();
        metrics.add_intake_time(Duration::from_millis(5));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches, (BATCH_HISTORY_CAP + 10) as u64);
        assert_eq!(snapshot.inserted_rows, 10 * (BATCH_HISTORY_CAP + 10) as u64);
        assert_eq!(snapshot.batch_ms.len(), BATCH_HISTORY_CAP);
        assert_eq!(snapshot.wire_bytes, 1024);
        assert_eq!(snapshot.events, 1);
        assert_eq!(snapshot.intake_nanos, 5_000_000);
    }

    #[test]
    fn interval_report_logs_and_returns_current_snapshot() {
        // Глобальные счётчики трогаем только чтением: проверяем, что отчёт
        // по пустому срезу не паникует и возвращает консистентный срез.
        let previous = Snapshot::empty();
        let current = report_interval(previous);
        assert_eq!(current.events, METRICS.snapshot().events);
    }
}
