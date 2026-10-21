-- Снос ключа источника (ADR-0005, тикет #29). Обратный ход корректен,
-- пока журнал не накопил строки разных источников с совпадающими
-- LSN/позициями: под старые констрейнты такие дубли не влезут.
DROP INDEX IF EXISTS changes_source_index;

ALTER TABLE public.changes DROP CONSTRAINT changes_position_table_schema_database_operation_unique;
ALTER TABLE public.changes
    ADD CONSTRAINT changes_position_table_schema_database_operation_unique
    UNIQUE (position, "table", schema, database, operation);

ALTER TABLE public.changes DROP CONSTRAINT changes_commit_lsn_event_index_unique;
ALTER TABLE public.changes
    ADD CONSTRAINT changes_commit_lsn_event_index_unique
    UNIQUE (commit_lsn, event_index);

ALTER TABLE public.changes DROP COLUMN "source";
