# Waltrail — журнал аудита изменений PostgreSQL

## Локальная разработка

```bash
cp env.example .env
docker compose up -d
scripts/register-source.sh
```

Последний шаг — [`scripts/register-source.sh`](scripts/register-source.sh) — идемпотентно регистрирует источник демо-стенда (`db-src`) в реестре через REST API: повторный запуск с той же строкой подключения завершается успехом, имя с другой строкой — ошибкой (URL источника неизменяем, ADR-0005). Сверка идёт по маскированному url из ответа API (схема и хост): креды, порт и имя базы в ответах не раскрываются (тикет #38). Скрипту нужны `curl` и `python3`; параметры — `scripts/register-source.sh ИМЯ URL`, окружение — `WALTRAIL_API_URL` (по умолчанию `http://localhost:8000`) и `WALTRAIL_SOURCE_URL`.

Compose поднимает минимальный стек:

| Сервис | Роль | Порт на хосте |
| ------ | ---- | ------------- |
| `db1` | БД аудита Waltrail (таблица `changes`, реестр источников + чекпоинты) | `54399` |
| `db-src` | Источник CDC — имитация БД клиента (`wal_level=logical`), подключается регистрацией через API | `54398` |
| `api` | REST API журнала и реестра источников | `8000` |
| `worker-cdc` | Супервизор: задача логической репликации на каждый источник реестра | — |

> Порт `8000` (api) публикуется на хост только для локальной разработки: в рабочем деплое API наружу не выставляется — см. [Безопасность](#безопасность) и [ADR-0007](docs/adr/0007-сетевая-изоляция-контура-реестра-источников.md).

Поддерживаемая версия СУБД — PostgreSQL 17: стенд и live-тесты выполняются на актуальном миноре ветки 17 (`postgres:17.11` на дату сверки 2026-10-22 — [релизы PostgreSQL](https://www.postgresql.org/docs/release/)); версии пересматриваются при выходе новых миноров или завершении поддержки ветки.

Единственный тракт инжеста — слот логической репликации (ADR-0001): воркер `worker-cdc` читает pgoutput-поток источника и пишет журнал в таблицу `changes`. API читает журнал только из `changes`: `GET /api/changes` (список с пагинацией, сортировкой и фильтрами — источник, `trace_id`, таблица, операция, время, вхождение в `context`) и `GET /api/changes/{id}` (детальная); Swagger — на `/swagger-ui`. Строки журнала помечены ключом источника (`source`): пустая строка — источник неизвестен (строки до переезда на мультиисточность); уникальные констрейнты журнала включают ключ, поэтому LSN/txid разных серверов не коллидируют (ADR-0005).

Реестр источников (ADR-0005) управляется через REST: `POST /api/sources` (регистрация — полный URL передаётся только здесь), `GET /api/sources` (список со статусами воркеров), `GET /api/sources/{name}`, `PATCH /api/sources/{name}` (только служебные таблицы, префикс MESSAGE и размер батча — URL неизменяем), `DELETE /api/sources/{name}` (помечает к удалению, `202`; исполнение — за воркером). Строка подключения хранится в реестре для воркера, но в ответах маскируется (тикет #38): видны схема и хост, креды, порт и имя базы скрыты (`postgresql://*****@db-client`) — анонимное чтение реестра не раскрывает креды. Воркер-супервизор опрашивает реестр и держит по задаче на каждый источник: зарегистрированный источник подхватывается без перезапуска — воркер сам создаёт у клиента публикацию `audit_<имя>` и слот `waltrail_<имя>`, помечает строки журнала ключом источника и пишет статус (подключен/ошибка, время последнего события) обратно в реестр.

Удаление исполняется воркером: задача помеченного источника останавливается, у клиента дропаются слот и публикация, строка реестра удаляется; строки журнала удалённого источника остаются в `changes`. Клиент недоступен — воркер повторяет дроп с backoff, состояние застрявшего удаления видно в реестре (`worker_state=deleting`, текст ошибки в `last_error`) до успеха: строка реестра не снимается, пока слот не дропнут — брошенный слот не копит WAL.

Сквозной сценарий «регистрация источника → эмит → журнал → REST-фильтр по trace_id»:

```bash
# Регистрируем db-src как источник журнала: воркер подхватит его без
# перезапуска и сам создаст у клиента публикацию и слот
# (то же вручную: curl -X POST http://localhost:8000/api/sources -H 'content-type: application/json' \
#  -d '{"name":"demo","url":"postgresql://postgres:postgres@db-src:5432/postgres"}')
scripts/register-source.sh

# Демо-таблица на источнике (публикация FOR ALL TABLES подхватит её сама);
# слот создаётся воркером при первом опросе реестра — после регистрации
# дайте стенду пару секунд, иначе изменения между регистрацией и созданием
# слота в журнал не попадут
docker compose exec db-src psql -U postgres -c \
  "CREATE TABLE IF NOT EXISTS demo (id int PRIMARY KEY, note text)"

# Изменение + контекст операции: pg_logical_emit_message в той же транзакции (ADR-0004).
# MESSAGE строк журнала не создаёт — обогащает context и trace_id строк своей транзакции.
docker compose exec db-src psql -U postgres -c \
  "BEGIN; INSERT INTO demo VALUES (1, 'демо'); \
   SELECT pg_logical_emit_message(true, '_audit', \
   '{\"trace_id\":\"demo-1\",\"user\":\"alice\"}'); COMMIT;"

# Журнал: фильтр по trace_id возвращает строку INSERT с контекстом MESSAGE
curl 'http://localhost:8000/api/changes?trace_id=demo-1'

# Статус источника: worker_state=connected, время последнего события
curl 'http://localhost:8000/api/sources/demo'
```

Воркер-супервизор применяет миграции и держит по задаче репликации на каждый источник реестра: подхват новых источников — без перезапуска, после остановки (Ctrl+C/SIGTERM) каждая задача возобновляет со своего чекпоинта. Настройки источников — в реестре (REST API, ADR-0005): строки подключения, служебные таблицы, префикс MESSAGE, размер батча; из окружения читаются только глобальные настройки — `CDC_RECONNECT_INITIAL_BACKOFF_MS`, `CDC_RECONNECT_MAX_BACKOFF_MS`, `CDC_TX_BUFFER_MAX_ROWS` (значения по умолчанию — в [`env.example`](env.example)).

> Слоты удерживают WAL источников: если воркер на dev-стенде больше не нужен, удалите источники через API (воркер дропнет слоты и публикации у клиентов) либо снесите слоты руками, чтобы WAL не копился:
>
> ```SQL
> SELECT pg_drop_replication_slot('waltrail_demo');
> ```

Подготовка источника для dev: достаточно `wal_level=logical` — публикация и слот создаются воркером автоматически при подхвате источника (регистрация — `POST /api/sources`, см. сквозной сценарий выше).

### PoC-бенчмарк пропускной способности

Генератор потока (200+ тыс. событий/сек узкими строками) и монитор стенда — в [`scripts/loadgen/`](scripts/loadgen); сводки по трём ступеням конвейера (walsender → intake → вставка) воркер пишет в лог каждые 10 с (цель `worker_cdc::metrics`):

```bash
python3 -m venv .venv && .venv/bin/pip install -r scripts/loadgen/requirements.txt
.venv/bin/python scripts/loadgen/generate.py --duration 60 --sessions 3 --rows-per-tx 10000 --payload-bytes 100
.venv/bin/python scripts/loadgen/monitor.py --duration 120 > monitor.csv
```

Методика, числа и вердикт гейта — в [`docs/benchmarks/0007-poc-пропускная-способность.md`](docs/benchmarks/0007-poc-пропускная-способность.md) (вердикт продублирован в [ADR-0003](docs/adr/0003-гейт-производительности-no-go-вставка-в-changes.md)).

**Внимание:** история миграций переписана под целевую схему журнала без мёртвых объектов (issue #11), а данные сервисов живут в именованных volumes. Если у вас остался старый volume — пересоздайте его:

```bash
docker compose down -v && docker compose up -d
```

> `down -v` удаляет локальные данные dev-стенда (в том числе volume БД аудита).

## Безопасность

Аутентификации в API нет; граница доверия — сеть ([ADR-0007](docs/adr/0007-сетевая-изоляция-контура-реестра-источников.md)): Waltrail разворачивается в изолированном контуре, API публикуется только в доверенной сети (приватная подсеть, внутренняя docker-сеть; операторы — через bastion/VPN) и **не выставляется в недоверенные сети**. Это осознанное уточнение решения «аутентификации нет» (ADR-0005): оно сформулировано вокруг чтения журнала и не покрывает жизненный цикл источников — мутации реестра (`POST`/`PATCH`/`DELETE /api/sources`) привилегированы, и анонимная мутация из недоверенной сети даёт атакующему:

- **blind-SSRF с ошибкой-оракулом** — `POST /api/sources` с произвольным url заставляет воркер стучаться на любой `host:port`; статус и текст ошибки видны в реестре (`GET /api/sources`) — по ним различают открытые и закрытые порты внутреннего периметра;
- **изменение репликационной конфигурации внутренних баз** — воркер создаёт у «источника» публикацию `audit_<имя>` и слот `waltrail_<имя>`; слот удерживает WAL базы;
- **остановку сбора одной командой** — `DELETE /api/sources/<имя>` останавливает сбор боевого источника и дропает у клиента слот и публикацию; повторная регистрация создаёт слот с текущей позиции WAL — непрочитанный хвост в журнал не попадает.

Нарушение изоляции — полный анонимный контроль над жизненным циклом источников плюс зондирование периметра; компенсирующих механизмов в коде нет, контроль сети (firewall/ACL) — обязанность оператора. Чтение журнала и реестра — принятый риск (ADR-0005), смягчённый маскированием url в ответах реестра (тикет #38). При появлении внешнего контура (доступ из недоверенной сети, мультиарендность) решение пересматривается: аутентификация мутаций или разделение API (ADR-0007).

## Переезд со старой одноисточниковой версии (runbook)

Версии до реестра источников читали ровно один клиентский источник, заданный при старте воркера: `SOURCE_DATABASE_URL` (строка подключения), `CDC_SLOT` (слот, по умолчанию `waltrail`), `CDC_PUBLICATION` (публикация, по умолчанию `audit`), `CDC_MESSAGE_PREFIX`, `CDC_IGNORE_TABLES`, `CDC_BATCH_SIZE`. В мультиисточниковой версии эти переменные удалены: единственный источник правды — реестр (REST API, ADR-0005), из окружения читаются только `DATABASE_URL`, логирование и глобальные параметры воркера (см. [`env.example`](env.example)).

1. Обновите сервисы: доставьте новые образы/бинарники и поднимите стек (`docker compose up -d`; миграции применяются на старте). Реестр пока пуст — старый слот `waltrail` у клиента продолжает жить и копить WAL; на время переезда это не страшно, дропнуть его сможем на шаге 4.
2. Перерегистрируйте прежний источник в реестре — тем же именем, каким хотите помечать его строки в журнале, и с той же строкой подключения, что была в `SOURCE_DATABASE_URL`:

   ```bash
   scripts/register-source.sh demo 'postgresql://postgres:postgres@db-client:5432/postgres'
   # или вручную: curl -X POST http://localhost:8000/api/sources -H 'content-type: application/json' \
   #  -d '{"name":"demo","url":"postgresql://postgres:postgres@db-client:5432/postgres"}'
   ```

   Воркер создаст у клиента публикацию `audit_<имя>` и слот `waltrail_<имя>` и продолжит сбор. Учтите: новый слот стартует с текущей позиции WAL клиента — непрочитанный хвост старого слота в журнал не попадает; если он нужен, вычитайте его прежней версией воркера до обновления.
3. Проверьте журнал: внесите тестовое изменение на клиенте и убедитесь, что оно попало в журнал с ключом источника (`worker_state=connected`, время последнего события — в `GET /api/sources/<имя>`):

   ```bash
   curl 'http://localhost:8000/api/changes?source=demo&per_page=5'
   curl 'http://localhost:8000/api/sources/demo'
   ```

4. Дропните старый слот и публикацию у клиента — пока слот `waltrail` жив, WAL клиента копится:

   ```SQL
   SELECT pg_drop_replication_slot('waltrail');
   DROP PUBLICATION IF EXISTS audit;
   ```

   Строки журнала, прочитанные старой версией до переезда, остаются в `changes` с пустым ключом источника (`source=""`).

## Подключение клиента (PostgreSQL)

1. Включите логический WAL и перезапустите базу (полный restart: `wal_level` не применяется через reload):
```SQL
    ALTER SYSTEM SET wal_level = logical;
    -- затем рестарт сервера, например: systemctl restart postgresql
```

2. Зарегистрируйте источник через API (`POST /api/sources`): воркер сам создаст у клиента публикацию `audit_<имя>` (FOR ALL TABLES) и слот `waltrail_<имя>` при подхвате. Если предпочитаете создать публикацию руками — воркер переиспользует существующую:
```SQL
-- Публикация "audit_<имя>" для всех таблиц (обычно не нужна — создаётся воркером)
CREATE PUBLICATION audit_<имя> FOR ALL TABLES;

-- Или только для выбранных таблиц
CREATE PUBLICATION audit_<имя> FOR TABLE [table1], [table2];

-- Полная репликационная идентичность: в журнал попадают образы "до" (UPDATE/DELETE)
ALTER TABLE [table1] REPLICA IDENTITY FULL;
ALTER TABLE [table2] REPLICA IDENTITY FULL;

-- Чтобы включить отслеживание изменений данных для новой таблицы:
ALTER PUBLICATION audit_<имя> ADD TABLE [table3];
ALTER TABLE [table3] REPLICA IDENTITY FULL;

-- Чтобы остановить отслеживание изменений данных для таблицы:
ALTER PUBLICATION audit_<имя> DROP TABLE [table3];
ALTER TABLE [table3] REPLICA IDENTITY DEFAULT;
```

3. Контекст операции передаётся через `pg_logical_emit_message` в собственной транзакции клиента (ADR-0004): ключ `trace_id` верхнего уровня payload становится колонкой журнала, а весь payload — контекстом всех изменений этой транзакции. MESSAGE строк журнала не создаёт: единственное содержимое журнала — изменения строк, событие без изменений строк в него не попадает (журнал событий приложение ведёт само):

```SQL
BEGIN;
UPDATE orders SET status = 'paid' WHERE id = 42;
SELECT pg_logical_emit_message(
    true,
    '_audit',
    '{"trace_id":"op-123","user":"alice","app":"my-app"}'
);
COMMIT;
```

4. Контекст из приложения — готовые сниппеты. Общий контракт (ADR-0002): payload — произвольный JSON, ключ `trace_id` верхнего уровня воркер копирует в одноимённую колонку журнала, остальное дословно уходит в `context`; префикс MESSAGE — настройка источника в реестре (по умолчанию `_audit`). Оба сниппета держат MESSAGE и изменения в одной транзакции:

TypeScript (`npm install pg`, драйвер [node-postgres](https://node-postgres.com/)):

```TypeScript
import { Pool, type PoolClient } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Контекст операции и изменения — одна транзакция (ADR-0004):
// MESSAGE обогащает context и trace_id строк именно этой транзакции.
async function withAuditContext<T>(
  traceId: string,
  context: Record<string, unknown>,
  work: (tx: PoolClient) => Promise<T>,
): Promise<T> {
  const tx = await pool.connect();
  try {
    await tx.query("BEGIN");
    await tx.query("SELECT pg_logical_emit_message(true, '_audit', $1)", [
      JSON.stringify({ trace_id: traceId, ...context }),
    ]);
    const result = await work(tx);
    await tx.query("COMMIT");
    return result;
  } catch (error) {
    await tx.query("ROLLBACK");
    throw error;
  } finally {
    tx.release();
  }
}

// Использование: "op-123" — в колонку trace_id журнала, остальное — в context.
// main-обёртка — чтобы сниппет работал и в CJS-, и в ESM-проекте.
async function main() {
  await withAuditContext("op-123", { user: "alice", app: "my-app" }, (tx) =>
    tx.query("UPDATE orders SET status = 'paid' WHERE id = 42"),
  );
  await pool.end();
}

main();
```

Python (`pip install "psycopg[binary]"`, драйвер [psycopg 3](https://www.psycopg.org/psycopg3/)). Контекст операции живёт в `ContextVar` — веб-приложение ставит его в middleware один раз на запрос, а emit вызывается уже внутри транзакции с изменениями:

```Python
import json
import os
from contextvars import ContextVar

import psycopg


class Audit:
    """Контекст операции для журнала Waltrail (ADR-0004).

    Контекст кладётся в ContextVar на время операции (например, в middleware),
    emit вызывается внутри транзакции с изменениями: MESSAGE обогащает
    context и trace_id строк именно этой транзакции.
    """

    _context_var: ContextVar[dict | None] = ContextVar("audit", default=None)

    @staticmethod
    def set_context(context: dict) -> None:
        Audit._context_var.set(context)

    @staticmethod
    def emit(conn: psycopg.Connection) -> None:
        """MESSAGE в текущей транзакции: trace_id — в колонку журнала, остальное — в context."""
        context = Audit._context_var.get()
        if context is None:
            return
        conn.execute(
            # Префикс "_audit" — настройка источника в реестре Waltrail (дефолт "_audit")
            "SELECT pg_logical_emit_message(true, '_audit', %s)",
            (json.dumps(context),),
        )


# Контекст текущей операции (в веб-приложении — в middleware, на каждый запрос)
Audit.set_context({"trace_id": "op-123", "user": "alice", "app": "my-app"})

# MESSAGE и изменения — одна транзакция
with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
    with conn.transaction():
        Audit.emit(conn)
        conn.execute("UPDATE orders SET status = 'paid' WHERE id = 42")
```

```mermaid
flowchart LR

Client[Client]

subgraph M_Audit
  W["Rust Worker CDC"]
  A["Rust API"]
  PG[(PostgreSQL<br>журнал)]
end

subgraph M_Any
  PG_ANY[(PostgreSQL<br>клиента)]
  A_Any["API"]
end

%% Client interaction
Client --> A

%% M_Audit internal
A -->|Read| PG
W -->|Write| PG

%% M_Any internal
A_Any <--> |Write/Read| PG_ANY

%% Cross-system flows
A_Any -.->|pg_logical_emit_message в транзакции| PG_ANY
PG_ANY -->|CDC: слот логической репликации pgoutput| W
```

## Эксплуатация

### Мониторинг слотов

Каждый источник реестра держит у клиента слот `waltrail_<имя>`; слот удерживает WAL, пока воркер его не подтвердит. Отставание копит лаг, инвалидация слота — потеря изменений. Готовый запрос — по всем слотам Waltrail на источнике:

```SQL
SELECT slot_name,
       active,
       wal_status,
       confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name LIKE 'waltrail\_%'
ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) DESC NULLS LAST;
```

- `lag` — объём WAL, который источник ещё не отдал в журнал; норма — около нуля, растёт при простое или лаге воркера.
- `wal_status`: `reserved` — норма; `extended` — лаг превысил `max_slot_wal_keep_size`, WAL ещё держится; `unreserved` — WAL уже может быть скинут чекпойнтом, слот на пороге инвалидации; `lost` (а `confirmed_flush_lsn` — `NULL`) — слот инвалидирован: изменения с момента инвалидации не могут быть прочитаны.
- `active = false` при живом воркере — задача источника не читает слот: статус и текст ошибки — в реестре (`GET /api/sources`).

Минимум алертинга: `wal_status <> 'reserved'` и `lag` выше порога (см. ниже).

### Порог max_slot_wal_keep_size

Порог задаётся на источнике и ограничивает WAL, который слот вправе удерживать:

```SQL
-- Стартовая рекомендация 10 ГиБ: вместить лаг за допустимый простой воркера
-- (деплой, отладка) с запасом; подберите по пиковой генерации WAL вашего
-- клиента и по поведению wal_status в мониторинге выше
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

Без лимита (дефолт `-1`) долгий простой воркера переполняет диск источника — это хуже ограниченной потери хвоста. При превышении порога `wal_status` проходит `extended` → `unreserved` → `lost`: слот инвалидирован, восстановление — по шагам ниже (воркер при этом повторяет попытки с backoff и держит статус `error` в реестре, остальные источники продолжают работать).

### Runbook: переключение источника на новый узел (failover)

Слот не переезжает на standby автоматически — поведение определяется версией PostgreSQL источника.

**PostgreSQL 17+ (failover slots)** — слот синхронизируется на standby и переживает `promote`:

1. На standby: `hot_standby = on`, `sync_replication_slots = on`; рекомендуется `hot_standby_feedback = on` ([дока PostgreSQL 17](https://www.postgresql.org/docs/17/logical-replication-failover.html)).
2. Слот должен быть создан с `failover = true`. Воркер создаёт слот без этого флага, поэтому для защищаемых источников создайте слот до регистрации источника — воркер переиспользует существующий слот (именованные аргументы — чтобы не перепутать порядок булевых флагов):

```SQL
SELECT * FROM pg_create_logical_replication_slot(
    slot_name => 'waltrail_<имя>',
    plugin => 'pgoutput',
    failover => true
);
```

3. После `promote` слот уже на новом primary: воркер переподключится по неизменному DSN (VIP/балансировщик) и продолжит с `confirmed_flush_lsn` слота — без потерь.

**PostgreSQL < 17 (и слоты без `failover = true`)** — слот на новом primary отсутствует:

- воркер восстановится сам: очередная попытка задачи обнаружит отсутствие слота и создаст новый с текущей позиции WAL — непрочитанный хвост (от последнего подтверждения до переключения) в журнал не попадёт;
- плановое переключение без потерь: дождитесь `lag ≈ 0` в мониторинге выше, остановите запись клиентов, переключайте — хвоста не будет.

Если при переключении сменился хост в строке подключения (не VIP): URL источника неизменяем (ADR-0005) — удалите источник (`DELETE /api/sources/<имя>`; воркер дропнет у клиента слот и публикацию по старому URL, строки журнала останутся в `changes`) и зарегистрируйте заново с тем же именем и новым URL — новый слот стартует с текущей позиции, непрочитанный хвост в журнал не попадает. Если старый узел окончательно недоступен и дроп исполнить нельзя, источник останется в `worker_state=deleting` — строка реестра не снимается, пока слот не дропнут (см. раздел о реестре выше).
