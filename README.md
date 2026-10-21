# Waltrail — журнал аудита изменений PostgreSQL

## Локальная разработка

```bash
cp env.example .env
docker compose up -d
```

Compose поднимает два PostgreSQL:

| Сервис | Роль | Порт на хосте |
| ------ | ---- | ------------- |
| `db1` | БД аудита Waltrail (таблицы `changes`, `audits`) | `54399` |
| `db-src` | Источник CDC — имитация БД клиента (`wal_level=logical`, публикация `audit` создаётся автоматически) | `54398` |

Остальные сервисы (Kafka, Redis, Debezium, kafka-ui) и легаси-крейты (`worker-app`, `worker-debezium`) работают как раньше — переход на CDC через pgoutput идёт поэтапно (см. issue #3). API читает журнал только из таблицы `changes`: `GET /api/changes` (список с пагинацией, сортировкой и фильтрами — `trace_id`, таблица, операция, время, вхождение в `context`) и `GET /api/changes/{id}` (детальная); Swagger — на `/swagger-ui`. Новый воркер CDC уже готов к использованию:

```bash
export SOURCE_DATABASE_URL=postgresql://postgres:postgres@localhost:54398/postgres
export DATABASE_URL=postgresql://postgres:postgres@localhost:54399/postgres
cargo run -p worker-cdc
```

Воркер сам создаёт слот репликации и применяет миграции, после остановки (Ctrl+C/SIGTERM) возобновляет с чекпоинта. Настройки — переменными окружения: `CDC_SLOT`, `CDC_PUBLICATION`, `CDC_IGNORE_TABLES`, `CDC_BATCH_SIZE` (значения по умолчанию — в [`env.example`](env.example)).

> Слот удерживает WAL источника: если воркер на dev-стенде больше не нужен, удалите слот, чтобы WAL не копился:
>
> ```SQL
> SELECT pg_drop_replication_slot('waltrail');
> ```

Подготовка источника для dev: публикация создаётся init-скриптом [`docker/init-source/01_публикация_аудита.sql`](docker/init-source/01_публикация_аудита.sql) при первом создании volume. Слот репликации создаётся воркером CDC автоматически при старте. Удалить (например, после пересоздания стенда):

```SQL
SELECT pg_drop_replication_slot('waltrail');
```

### PoC-бенчмарк пропускной способности

Генератор потока (200+ тыс. событий/сек узкими строками) и монитор стенда — в [`scripts/loadgen/`](scripts/loadgen); сводки по трём ступеням конвейера (walsender → intake → вставка) воркер пишет в лог каждые 10 с (цель `worker_cdc::metrics`):

```bash
python3 -m venv .venv && .venv/bin/pip install -r scripts/loadgen/requirements.txt
.venv/bin/python scripts/loadgen/generate.py --duration 60 --sessions 3 --rows-per-tx 10000 --payload-bytes 100
.venv/bin/python scripts/loadgen/monitor.py --duration 120 > monitor.csv
```

Методика, числа и вердикт гейта — в [`docs/benchmarks/0007-poc-пропускная-способность.md`](docs/benchmarks/0007-poc-пропускная-способность.md) (вердикт продублирован в [ADR-0003](docs/adr/0003-гейт-производительности-no-go-вставка-в-changes.md)).

**Внимание:** история миграций переписана под целевую схему журнала (без накопленных ALTER-шагов), а данные сервисов живут в именованных volumes. Если у вас остался старый volume — пересоздайте его:

```bash
docker compose down -v && docker compose up -d
```

> `down -v` удаляет локальные данные dev-стенда (в том числе volume БД аудита).
>
> Сервис БД аудита переименован `db` → `db1` (как в `env.example`). Если Debezium-коннектор был зарегистрирован с `database.hostname: db` — перерегистрируйте его с `db1` после пересоздания стенда.

### Postgres (Клиент)
1. Запустите следующую команду SQL, чтобы изменить уровень WAL с реплики на логический и перезапустить вашу базу данных:
```SQL
    ALTER SYSTEM SET wal_level = logical;
```

__Обязательно нужно перезагрузить postgres!!!__

2. Создание публикации.
```SQL
-- Создайте ПУБЛИКАЦИЮ "audit", чтобы включить логическую репликацию для всех таблиц
CREATE PUBLICATION audit FOR ALL TABLES;

-- Создайте ПУБЛИКАЦИЮ "audit", чтобы включить логическую репликацию для выбранных таблиц
CREATE PUBLICATION audit FOR TABLE [table1], [table2];

-- Установите ПОЛНУЮ ИДЕНТИФИКАЦИЮ РЕПЛИКИ для таблиц, чтобы отслеживать состояние "до" изменений строк базы данных
ALTER TABLE [table1] REPLICA IDENTITY FULL;
ALTER TABLE [table2] REPLICA IDENTITY FULL;


-- Чтобы включить отслеживание изменений данных для новой таблицы:
ALTER PUBLICATION audit ADD TABLE [table3];
ALTER TABLE [table3] REPLICA IDENTITY FULL;

-- Чтобы остановить отслеживание изменений данных для таблицы:

ALTER PUBLICATION audit DROP TABLE [table3];
ALTER TABLE [table3] REPLICA IDENTITY DEFAULT;

```

3. Настройки для добавления в Debezium для отслеживания.
```JSON
{
    "name": ${name},
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": ${hostname},
        "database.port": ${port} or "5432",
        "database.user": ${username},
        "database.password": ${password},
        "database.dbname": ${db_name},
        "topic.prefix": "audit",
        "plugin.name": "pgoutput",
        "table.exclude.list": "public.alembic_version,",
        "snapshot.mode": "never",
        "transforms": "Reroute",
        "transforms.Reroute.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.Reroute.regex": "audit\\..*",
        "transforms.Reroute.replacement": "audit",
        "heartbeat.interval.ms": "5000"
    },
}
```
```mermaid
flowchart LR

Client[Client]

subgraph Debezium
  Api_D["Java API"]
  D["Debezium Connect"]
end

subgraph M_Audit
  W["Rust Worker"]
  A["Rust API"]
  PG[(PostgreSQL)]
end

subgraph M_Any
  PG_ANY[(PostgreSQL)]
  A_Any["API"]
end

K[(Kafka)]

%% Client interaction
Client --> A

%% M_Audit internal
A -->|Read| PG
W -->|Write| PG

%% M_Any internal
A_Any <--> |Write/Read| PG_ANY

%% Cross-system flows
M_Any -.->|Добавление данных о БД в коннектор| Api_D
PG_ANY -->|CDC| D
D -->|Debezium скидывает данные по топикам в Kafka| K

K -->|Worker читает топики и обрабатывает| W
A_Any -.->|Данные о выполнении запроса| K
```
