#!/usr/bin/env python3
# Монитор стенда для PoC-бенчмарка Waltrail (тикет #10): сэмплит источник CDC
# (скорость WAL, состояние слота репликации) и БД аудита (объём журнала).
#
# Требования: Python 3.9+, psycopg 3 (scripts/loadgen/requirements.txt).
# Запуск:
#   .venv/bin/python scripts/loadgen/monitor.py --duration 120 > bench_monitor.csv
#
# Формат вывода (CSV, разделитель «;»):
# elapsed_s;wal_mib_s;wal_total_mib;lag_mib;wal_status;changes_total;changes_ps;audit_table_mib

import argparse
import sys
import time
from dataclasses import dataclass

import psycopg

DEFAULT_SOURCE_DSN = "postgresql://postgres:postgres@localhost:54398/postgres"
DEFAULT_AUDIT_DSN = "postgresql://postgres:postgres@localhost:54399/postgres"
DEFAULT_SLOT = "waltrail"

MIB = 1024.0 * 1024.0

SOURCE_SQL = (
    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s::pg_lsn)::bigint AS wal_bytes, "
    "pg_current_wal_lsn()::text"
)
# Слот репликации живёт на источнике, не на БД аудита.
SLOT_SQL = (
    "SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)"
    "::bigint AS lag_bytes, "
    "wal_status "
    "FROM pg_replication_slots WHERE slot_name = %s"
)
CHANGES_SQL = "SELECT count(*)::bigint FROM changes"
SIZE_SQL = "SELECT pg_total_relation_size('changes')::bigint"


@dataclass
class SampleState:
    """Состояние между сэмплами монитора: точки отсчёта дельт."""

    at: float
    wal_lsn: str
    wal_total_mib: float
    changes: int
    started: float


def fetch_row(conn, query, params=()):
    """Первая строка результата; пустой результат — ошибка."""
    row = conn.execute(query, params).fetchone()
    if row is None:
        raise RuntimeError(f"запрос не вернул строк: {query!r}")
    return row


def sample(source, audit, slot, state):
    """Один сэмпл метрик; возвращает строку CSV и новое состояние."""
    now = time.monotonic()
    wal_row = fetch_row(source, SOURCE_SQL, (state.wal_lsn,))
    wal_bytes, wal_lsn = int(wal_row[0]), str(wal_row[1])
    wal_delta = max(wal_bytes, 0)  # отрицательная дельта — ротация WAL
    slot_row = fetch_row(source, SLOT_SQL, (slot,))
    lag_bytes, wal_status = int(slot_row[0]), slot_row[1] or "—"
    changes = int(fetch_row(audit, CHANGES_SQL)[0])
    table_mib = int(fetch_row(audit, SIZE_SQL)[0]) / MIB

    elapsed = now - state.at
    wal_mib_s = wal_delta / MIB / elapsed
    changes_ps = max(changes - state.changes, 0) / elapsed
    line = (
        f"{now - state.started:.0f};{wal_mib_s:.1f};"
        f"{state.wal_total_mib + wal_delta / MIB:.1f};"
        f"{lag_bytes / MIB:.1f};{wal_status};"
        f"{changes};{changes_ps:.0f};{table_mib:.0f}"
    )
    return line, SampleState(
        at=now,
        wal_lsn=wal_lsn,
        wal_total_mib=state.wal_total_mib + wal_delta / MIB,
        changes=changes,
        started=state.started,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Монитор стенда PoC-бенчмарка Waltrail"
    )
    parser.add_argument("--source-dsn", default=DEFAULT_SOURCE_DSN)
    parser.add_argument("--audit-dsn", default=DEFAULT_AUDIT_DSN)
    parser.add_argument("--slot", default=DEFAULT_SLOT)
    parser.add_argument(
        "--interval", type=float, default=5.0, help="период сэмпла, сек"
    )
    parser.add_argument(
        "--duration", type=float, default=120.0, help="длительность, сек"
    )
    args = parser.parse_args()

    started = time.monotonic()
    print(
        "elapsed_s;wal_mib_s;wal_total_mib;lag_mib;wal_status;"
        "changes_total;changes_ps;audit_table_mib",
        flush=True,
    )
    with (
        psycopg.connect(args.source_dsn, autocommit=True) as source,
        psycopg.connect(args.audit_dsn, autocommit=True) as audit,
    ):
        # Точки отсчёта: текущий LSN и объём журнала без дельт.
        state = SampleState(
            at=started,
            wal_lsn=str(fetch_row(source, "SELECT pg_current_wal_lsn()::text")[0]),
            wal_total_mib=0.0,
            changes=int(fetch_row(audit, CHANGES_SQL)[0]),
            started=started,
        )
        while time.monotonic() - state.started < args.duration:
            time.sleep(args.interval)
            try:
                line, state = sample(source, audit, args.slot, state)
            except psycopg.Error as error:
                print(f"ошибка сэмпла: {error}", file=sys.stderr, flush=True)
                continue
            print(line, flush=True)


if __name__ == "__main__":
    main()
