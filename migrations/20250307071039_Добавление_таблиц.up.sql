-- Add up migration script here
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS public.changes (
    "id" uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    "database" VARCHAR(255) NOT NULL,
    "schema" VARCHAR(255) NOT NULL,
    "table" VARCHAR(255) NOT NULL,
    "primary_key" VARCHAR(255) NULL,
    "operation" TEXT NOT NULL,
    "before" JSONB NULL DEFAULT '{}',
    "after" JSONB NULL DEFAULT '{}',
    "context" JSONB DEFAULT '{}',
    "committed_at" TIMESTAMPTZ (0),
    "queued_at" TIMESTAMPTZ (0),
    "created_at" TIMESTAMPTZ (0) DEFAULT NOW (),
    "transaction_id" BIGINT NOT NULL,
    "position" BIGINT NOT NULL,
    "commit_lsn" BIGINT NULL,
    "event_index" INT NULL,
    "trace_id" VARCHAR(255) NULL,
    CONSTRAINT changes_position_table_schema_database_operation_unique UNIQUE (position, "table", schema, database, operation),
    CONSTRAINT changes_commit_lsn_event_index_unique UNIQUE (commit_lsn, event_index)
);

CREATE INDEX changes_after_index ON public.changes USING GIN ("after" jsonb_path_ops);

CREATE INDEX changes_before_index ON public.changes USING GIN ("before" jsonb_path_ops);

CREATE INDEX changes_context_index ON public.changes USING GIN ("context" jsonb_path_ops);

CREATE INDEX changes_operation_index ON public.changes (operation);

CREATE INDEX changes_primary_key_index ON public.changes (primary_key);

CREATE INDEX changes_table_index ON public.changes ("table");

CREATE INDEX changes_committed_at_index ON public.changes (committed_at);

COMMENT ON COLUMN public.changes.id IS 'Уникальный идентификатор записи об изменении';

COMMENT ON COLUMN public.changes.database IS 'Имя базы данных, где хранилась измененная запись';

COMMENT ON COLUMN public.changes.schema IS 'Имя схемы, где хранилась измененная запись';

COMMENT ON COLUMN public.changes.table IS 'Имя таблицы, где хранилась измененная запись';

COMMENT ON COLUMN public.changes.primary_key IS 'Уникальный идентификатор измененной записи (опционально)';

COMMENT ON COLUMN public.changes.operation IS 'Тип операции: CREATE, UPDATE, DELETE, TRUNCATE или MESSAGE';

COMMENT ON COLUMN public.changes.before IS 'Значения записи до изменения';

COMMENT ON COLUMN public.changes.after IS 'Значения записи после изменения';

COMMENT ON COLUMN public.changes.context IS 'Контекст приложения, переданный через pg_logical_emit_message (ADR-0002)';

COMMENT ON COLUMN public.changes.committed_at IS 'Когда запись была изменена';

COMMENT ON COLUMN public.changes.queued_at IS 'Когда изменение было считано из WAL';

COMMENT ON COLUMN public.changes.created_at IS 'Когда запись об изменении была сохранена в базе данных';

COMMENT ON COLUMN public.changes.transaction_id IS 'ID транзакции PostgreSQL';

COMMENT ON COLUMN public.changes.position IS 'Позиция в WAL PostgreSQL (LSN события)';

COMMENT ON COLUMN public.changes.commit_lsn IS 'LSN коммита транзакции источника (заполняет воркер CDC)';

COMMENT ON COLUMN public.changes.event_index IS 'Порядковый номер события внутри транзакции источника (для дедупликации вместе с commit_lsn)';

COMMENT ON COLUMN public.changes.trace_id IS 'Идентификатор сквозной операции приложения (из MESSAGE); объединяет все строки одной операции';

CREATE INDEX changes_trace_id_index ON public.changes (trace_id);
