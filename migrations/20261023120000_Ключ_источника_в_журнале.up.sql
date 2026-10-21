-- Ключ источника в журнале (ADR-0005, тикет #29): строки журнала
-- помечаются именем источника. Пустая строка — источник неизвестен
-- (строки, сохранённые до переезда на мультиисточность); воркер начнёт
-- заполнять колонку с тикета подхвата источников.
-- Размер 54 — как имя в реестре `sources`: 63 (NAMEDATALEN-1) минус
-- префикс слота `waltrail_`.
ALTER TABLE public.changes ADD COLUMN "source" VARCHAR(54) NOT NULL DEFAULT '';

-- Уникальные констрейнты журнала пересоздаются с участием ключа
-- источника: LSN и txid разных серверов коллидируют, без ключа вставка
-- второго источника падает (ADR-0005). Пустая строка ведёт себя как
-- обычное значение, поэтому дедупликация replay-дублей воркера
-- (ON CONFLICT DO NOTHING) сохраняется и в переходный период.
ALTER TABLE public.changes DROP CONSTRAINT changes_position_table_schema_database_operation_unique;
ALTER TABLE public.changes
    ADD CONSTRAINT changes_position_table_schema_database_operation_unique
    UNIQUE ("source", position, "table", schema, database, operation);

ALTER TABLE public.changes DROP CONSTRAINT changes_commit_lsn_event_index_unique;
ALTER TABLE public.changes
    ADD CONSTRAINT changes_commit_lsn_event_index_unique
    UNIQUE ("source", commit_lsn, event_index);

-- Фильтр чтения журнала по источнику — равенство по ключу.
CREATE INDEX changes_source_index ON public.changes ("source");

COMMENT ON COLUMN public.changes."source" IS 'Имя источника журнала (ADR-0005); пустая строка — источник неизвестен, строки до переезда на мультиисточность';
