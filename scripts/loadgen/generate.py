#!/usr/bin/env python3
# Генератор потока изменений для PoC-бенчмарка Waltrail (тикет #10).
#
# Создаёт на источнике CDC таблицу (если её нет) и генерирует поток
# INSERT/UPDATE/DELETE с «аудитными» (~1 КБ) или узкими строками. Строки
# порождаются на стороне PostgreSQL (generate_series), поэтому узкое место
# генератора — сам источник, а не Python: несколько параллельных сессий
# выдают 200+ тыс. событий/сек.
#
# Требования: Python 3.9+, psycopg 3 (scripts/loadgen/requirements.txt).
# Запуск:
#   python3 -m venv .venv && .venv/bin/pip install -r scripts/loadgen/requirements.txt
#   .venv/bin/python scripts/loadgen/generate.py --help
#
# Каждая транзакция: опциональный MESSAGE (pg_logical_emit_message, ADR-0002),
# затем батч DML одним оператором. События считаются по фактически
# изменённым строкам (rowcount), MESSAGE — отдельно.

import argparse
import json
import re
import signal
import sys
import threading
import time

import psycopg
from psycopg import sql

# Префикс MESSAGE-событий приложения (ADR-0002) — тот же, что в воркере.
AUDIT_MESSAGE_PREFIX = "_audit"

TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Генератор потока изменений для PoC-бенчмарка Waltrail"
    )
    parser.add_argument(
        "--dsn",
        default="postgresql://postgres:postgres@localhost:54398/postgres",
        help="DSN источника CDC (по умолчанию dev-стенд db-src)",
    )
    parser.add_argument(
        "--table",
        default="public.loadgen_events",
        help="таблица генератора в виде schema.table (создаётся, если нет)",
    )
    parser.add_argument("--sessions", type=int, default=4, help="параллельные сессии")
    parser.add_argument(
        "--rows-per-tx", type=int, default=5000, help="строк DML на транзакцию"
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=1024,
        help="целевой размер payload строки, байт (1024 — «аудитная», 100 — узкая)",
    )
    parser.add_argument(
        "--update-percent",
        type=float,
        default=0.0,
        help="доля транзакций с UPDATE вместо INSERT (рекомендуется --sessions 1)",
    )
    parser.add_argument(
        "--delete-percent",
        type=float,
        default=0.0,
        help="доля транзакций с DELETE вместо INSERT (рекомендуется --sessions 1)",
    )
    parser.add_argument(
        "--message-every",
        type=int,
        default=0,
        help="emit MESSAGE каждую N-ю транзакцию (0 — выключено)",
    )
    parser.add_argument(
        "--duration", type=float, default=60.0, help="длительность генерации, сек"
    )
    args = parser.parse_args()

    # Имя таблицы попадает в SQL только через sql.Identifier: регэксп —
    # ранняя валидация аргумента, экранирование делает psycopg.
    if not TABLE_NAME_RE.match(args.table):
        parser.error(f"--table: ожидается schema.table, получено {args.table!r}")
    if args.sessions < 1 or args.rows_per_tx < 1 or args.payload_bytes < 32:
        parser.error("--sessions/--rows-per-tx >= 1, --payload-bytes >= 32")
    if not (
        0.0 <= args.update_percent <= 100.0 and 0.0 <= args.delete_percent <= 100.0
    ):
        parser.error("доли UPDATE/DELETE задаются в процентах 0..100")
    if args.update_percent + args.delete_percent > 100.0:
        parser.error("сумма долей UPDATE и DELETE не больше 100")
    return args


def table_ident(table: str) -> sql.Identifier:
    """Квалифицированный идентификатор таблицы `schema.table`."""
    return sql.Identifier(*table.split("."))


def payload_expr(bytes_size: int) -> sql.SQL:
    """SQL-выражение случайного payload заданного размера: 32-байтовые блоки md5."""
    blocks = max(1, bytes_size // 32)
    remainder = bytes_size - blocks * 32
    expr = f"repeat(md5(random()::text), {blocks})"
    if remainder:
        expr += f" || left(md5(random()::text), {remainder})"
    # Выражение собрано из целочисленных аргументов, идентификаторов и
    # пользовательских строк в нём нет — LiteralString-ограничение psycopg
    # здесь обходится сознательно.
    return sql.SQL(expr)  # ty: ignore[invalid-argument-type]


def fetch_scalar(conn, query, params=()):
    """Первая колонка первой строки результата; пустой результат — ошибка."""
    row = conn.execute(query, params).fetchone()
    if row is None:
        raise RuntimeError(f"запрос не вернул строк: {query!r}")
    return row[0]


def setup_table(conn, table):
    """Создаёт таблицу генератора, включает REPLICA IDENTITY FULL.

    Возвращает имя последовательности идентификатора (для выбора
    диапазона UPDATE/DELETE).
    """
    ident = table_ident(table)
    with conn.transaction():
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                "payload text NOT NULL, "
                "created_at timestamptz NOT NULL DEFAULT now())"
            ).format(ident)
        )
        conn.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(ident))
    # pg_get_serial_sequence принимает имя таблицы строкой-параметром,
    # а не идентификатором в тексте запроса.
    sequence = fetch_scalar(conn, "SELECT pg_get_serial_sequence(%s, 'id')", (table,))
    if not sequence:
        raise RuntimeError(f"у таблицы {table} нет последовательности id")
    return str(sequence)


class Stats:
    """Общие счётчики генератора: строки, транзакции, MESSAGE."""

    def __init__(self):
        self.lock = threading.Lock()
        self.rows = 0
        self.transactions = 0
        self.messages = 0

    def add(self, rows, messages):
        with self.lock:
            self.rows += rows
            self.transactions += 1
            self.messages += messages

    def totals(self):
        with self.lock:
            return self.rows, self.transactions, self.messages


def session_loop(args, table, sequence, stop, stats, session_id):
    """Цикл сессии: транзакции DML до сигнала остановки."""
    ident = table_ident(table)
    insert_query = sql.SQL(
        "INSERT INTO {} (payload) SELECT {} FROM generate_series(1, %s)"
    ).format(ident, payload_expr(args.payload_bytes))
    update_query = sql.SQL(
        "UPDATE {} SET payload = {} WHERE id BETWEEN %s AND %s"
    ).format(ident, payload_expr(args.payload_bytes))
    delete_query = sql.SQL("DELETE FROM {} WHERE id BETWEEN %s AND %s").format(ident)
    message_query = "SELECT pg_logical_emit_message(true, %s, %s)"
    last_id_query = sql.SQL("SELECT last_value FROM {}").format(
        sql.Identifier(*sequence.split("."))
    )

    tx_number = 0
    try:
        with psycopg.connect(args.dsn) as conn:
            while not stop.is_set():
                tx_number += 1
                emit_message = (
                    args.message_every > 0 and tx_number % args.message_every == 0
                )
                roll = (tx_number * 2654435761 + session_id * 40503) % 10_000
                update = roll < args.update_percent * 100
                delete = (
                    not update
                    and roll < (args.update_percent + args.delete_percent) * 100
                )
                rows = 0
                with conn.transaction():
                    if emit_message:
                        payload = json.dumps(
                            {
                                "trace_id": f"bench-{session_id}-{tx_number}",
                                "user": f"loadgen-{session_id}",
                                "method": "POST",
                                "path": "/api/benchmark",
                                "status_code": 200,
                            }
                        )
                        conn.execute(message_query, (AUDIT_MESSAGE_PREFIX, payload))
                    if update or delete:
                        # Свои последние строки: с одной сессией блок id
                        # непрерывен, конфликтов блокировок с другими сессиями нет.
                        high = int(fetch_scalar(conn, last_id_query))
                        low = max(1, high - args.rows_per_tx + 1)
                        query = update_query if update else delete_query
                        rows = conn.execute(query, (low, high)).rowcount
                    else:
                        conn.execute(insert_query, (args.rows_per_tx,))
                        rows = args.rows_per_tx
                stats.add(rows, 1 if emit_message else 0)
    except psycopg.Error as error:
        print(f"[сессия {session_id}] ошибка: {error}", file=sys.stderr, flush=True)


def main():
    args = parse_args()
    table = args.table
    stop = threading.Event()
    stats = Stats()

    with psycopg.connect(args.dsn) as conn:
        sequence = setup_table(conn, table)
        start_rows = int(
            fetch_scalar(
                conn, sql.SQL("SELECT count(*) FROM {}").format(table_ident(table))
            )
        )

    print(
        f"генерация: таблица {table}, сессий {args.sessions}, "
        f"{args.rows_per_tx} строк/транзакцию, payload ~{args.payload_bytes} Б, "
        f"UPDATE {args.update_percent:.1f}%, DELETE {args.delete_percent:.1f}%, "
        f"MESSAGE каждые {args.message_every or '—'} транзакций, "
        f"длительность {args.duration:.0f} с",
        flush=True,
    )

    threads = [
        threading.Thread(
            target=session_loop,
            args=(args, table, sequence, stop, stats, index),
            daemon=True,
        )
        for index in range(args.sessions)
    ]
    for thread in threads:
        thread.start()

    def request_stop(signum, _frame):
        print("\nполучен сигнал остановки — завершаем текущие транзакции", flush=True)
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    started = time.monotonic()
    previous_rows, previous_at = 0, started
    peak_rate = 0.0
    while time.monotonic() - started < args.duration and not stop.is_set():
        time.sleep(1.0)
        now = time.monotonic()
        rows = stats.totals()[0]
        rate = (rows - previous_rows) / max(now - previous_at, 1e-9)
        peak_rate = max(peak_rate, rate)
        print(
            f"[{now - started:6.0f} с] {rows:>9} строк | {rate:>10.0f} строк/с",
            flush=True,
        )
        previous_rows, previous_at = rows, now

    stop.set()
    for thread in threads:
        thread.join(timeout=60)

    elapsed = time.monotonic() - started
    rows, transactions, messages = stats.totals()
    print(
        f"\nитог: {rows} строк за {elapsed:.1f} с "
        f"(в среднем {rows / elapsed:.0f} строк/с, пик {peak_rate:.0f} строк/с), "
        f"транзакций {transactions}, MESSAGE {messages}",
        flush=True,
    )
    print(f"строк в таблице до генерации: {start_rows}", flush=True)


if __name__ == "__main__":
    main()
