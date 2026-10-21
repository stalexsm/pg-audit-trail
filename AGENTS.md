# AGENTS.md

Правила для ИИ-агентов, работающих с этим репозиторием.

## Проект

Журнал аудита на Rust: workspace (edition 2024) с общей версией и общими зависимостями. Три крейта:

- `crates/api` — REST API (axum, sqlx, casbin, JWT, Swagger через utoipa): чтение журнала, авторизация.
- `crates/worker-app` — консюмер Kafka-топика `audit.app`: обогащает событие приложения `response_body` из Redis (по `request_id`), пишет в таблицу `audits`; идемпотентность через `ON CONFLICT DO NOTHING`.
- `crates/worker-debezium` — консюмер топика `audit` (CDC-конверт Debezium: `source`, `before`/`after`, `op`, `ts_ms`), пишет в таблицу `changes`.

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
- Ошибки — `anyhow`; в каждом крейте свой псевдоним `Result` (в api — `AppResult`), распространение через `?`.
- Логирование — `tracing` (`info!` / `warn!` / `error!`), уровень через `RUST_LOG`.
- Остановка воркеров — по `CancellationToken` в ответ на SIGTERM; новые фоновые задачи подключай к этой же схеме.
- Батчевые SQL-вставки — через `sqlx::QueryBuilder::push_values` с транзакцией и retry.
- Комментарии, имена, коммиты — на русском, как во всём проекте.

## Миграции

- Пара файлов в `migrations/`: `YYYYMMDDHHMMSS_Название.up.sql` + обязательный парный `.down.sql`.
- Миграции применяются автоматически при старте всех трёх сервисов (`sqlx::migrate!`) — после смены схемы достаточно перезапуска, отдельная команда не нужна.
- Схему видят все три крейта сразу: изменение таблицы может сломать и API, и оба воркера.

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
