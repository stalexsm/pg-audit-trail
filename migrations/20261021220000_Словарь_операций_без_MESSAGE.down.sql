-- Возврат словаря операций и комментария context к состоянию ADR-0002.
COMMENT ON COLUMN public.changes.operation IS 'Тип операции: CREATE, UPDATE, DELETE, TRUNCATE или MESSAGE';
COMMENT ON COLUMN public.changes.context IS 'Контекст приложения, переданный через pg_logical_emit_message (ADR-0002)';
