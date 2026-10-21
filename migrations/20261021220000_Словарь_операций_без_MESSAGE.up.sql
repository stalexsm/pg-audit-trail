-- ADR-0004 (тикет #20): MESSAGE — только канал контекста, строк журнала
-- не создаёт. Из словаря операций журнала убрано MESSAGE; заодно комментарий
-- context ссылается на актуальный ADR.
COMMENT ON COLUMN public.changes.operation IS 'Тип операции: CREATE, UPDATE, DELETE или TRUNCATE';
COMMENT ON COLUMN public.changes.context IS 'Контекст приложения, переданный через pg_logical_emit_message (ADR-0004)';
