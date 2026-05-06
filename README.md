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

@mermaid
```
