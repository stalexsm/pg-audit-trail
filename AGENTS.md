# AGENTS.md

Правила для ИИ-агентов, работающих с этим репозиторием.

## Проект

Журнал аудита на Rust: workspace (edition 2024) с общей версией и общими зависимостями. Идёт поэтапный переход на CDC через pgoutput (issue #3): новые крейты уже в работе, легаси удаляется в тикете #11.

- `crates/pgoutput` — парсер протокола pgoutput: Relation (кэш отношений, переживает ALTER TABLE), Insert/Update/Delete/Truncate → события с JSON-образами и ключом; без инфраструктурных зависимостей, проверяется живым PostgreSQL.
- `crates/worker-cdc` — консюмер логической репликации (ADR-0001): слот источника → таблица `changes`; бутстрап (создание слота, чекпоинт), батчевая вставка в транзакции с чекпоинтом, ack слота только после коммита; конфиг — крейт `config` из окружения (`CDC_*`), остановка по `CancellationToken`.
- `crates/api` — REST API (axum, sqlx, casbin, JWT, Swagger через utoipa): чтение журнала, авторизация.
- `crates/worker-app`, `crates/worker-debezium` — легаси-консюмеры Kafka/Redis/Debezium: работают до удаления (issue #11), новых фич в них не заводить.

Инфраструктура: PostgreSQL, Kafka, Redis, Debezium, kafka-ui. Схема потоков — в `README.md`, переменные окружения — в `env.example`.

## Обязательная проверка перед завершением

Задача с изменением кода считается выполненной только когда все команды прошли из корня репозитория без ошибок и предупреждений:

```bash
cargo fmt --check
cargo clippy --all --all-targets
cargo test --workspace
```

Предупреждения clippy исправляй в коде; `#[allow(...)]` допустим только с комментарием-обоснованием рядом.

Падающий тест — это дефект: исправляй код или тест так, чтобы вся проверка проходила, прежде чем сообщать о завершении. Новую логику сопровождай тестами (`#[tokio::test]` для async-кода); при тестах, требующих инфраструктуры (PostgreSQL, Kafka, Redis), подними её через `docker compose up -d`.

## Локальный запуск

1. `cp env.example .env` — заполненный `.env` подхватывается автоматически (dotenv).
2. `docker compose up -d` — поднимает инфраструктуру.
3. `cargo run -p api` (или `-p worker-app`, `-p worker-debezium`).

Гоча: `rdkafka` собирается с feature `cmake-build` — в системе должны быть установлены cmake и тулчейн C/C++.

## Конвенции кода

- Зависимости объявляй в `[workspace.dependencies]` корневого `Cargo.toml`, в крейтах подключай через `dep.workspace = true`.
- Ошибки — `anyhow`; в каждом крейте свой псевдоним `Result` (в api — `AppResult`), распространение через `?`. Исключение — библиотечный крейт `pgoutput`: собственный тип ошибки `PgOutputError` (библиотечный шов без зависимости `anyhow`).
- Логирование — `tracing` (`info!` / `warn!` / `error!`), уровень через `RUST_LOG`.
- Остановка воркеров — по `CancellationToken` в ответ на SIGTERM; новые фоновые задачи подключай к этой же схеме.
- Батчевые SQL-вставки — через `sqlx::QueryBuilder::push_values` с транзакцией и retry.
- Числа в бизнес-логике — только именованные константы или настройки (магических чисел в коде не оставлять).
- Комментарии — на русском; у публичных сервисов — docstring с описанием контракта.
- Комментарии, имена, коммиты — на русском, как во всём проекте.

## Миграции

- Пара файлов в `migrations/`: `YYYYMMDDHHMMSS_Название.up.sql` + обязательный парный `.down.sql` (создание — `sqlx migrate add -r <название>`).
- Миграции применяются автоматически при старте сервисов (`sqlx::migrate!`) — после смены схемы достаточно перезапуска, отдельная команда не нужна.
- Изменения миграций проверяй кодемодой:
  1. Цикл `sqlx migrate run` → `sqlx migrate revert` → `sqlx migrate run` — up/down обязан проходить без ошибок; статус применения — `sqlx migrate info`.
  2. Подключение — `--database-url` или `DATABASE_URL` стенда (sqlx-cli читает и `.env`); каталог миграций — `--source migrations`.
  3. Схему после применения сверь с ожидаемой через psql: колонки (`information_schema.columns`), индексы (`pg_indexes`), констрейнты (`pg_constraint`) — в ответ возвращай только расхождения в формате «объект: ожидалось / фактически».
- Схему видят все крейта сразу: изменение таблицы может сломать и API, и воркеры.

## Ветки, коммиты и релизы

- Любая работа — в новой ветке, созданной от актуального `main`; прямые коммиты в `main` запрещены. Имя ветки — `тип/краткое-описание` латиницей, по типу будущего коммита: `feat/enrich-response-body`, `fix/redis-retry`.
- Conventional Commits с русским описанием: `feat:`, `fix:`, `doc:`, `build:`, `chore(deps):`.
- `CHANGELOG.md` генерируется git-cliff по `cliff.toml` — не редактируй его вручную.
- Версия задаётся один раз в `[workspace.package]`; релиз — тег вида `v0.3.0`.

## Agent skills

### Issue tracker

Issues live in GitHub Issues (`gh` CLI) at `stalexsm/waltrail`. See `docs/agents/issue-tracker.md`.

### Triage labels

Standard five-role vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` at the repo root + `docs/adr/`. See `docs/agents/domain.md`.
