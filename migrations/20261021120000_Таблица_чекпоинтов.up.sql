-- Таблица чекпоинтов воркеров: координата потока изменений, до которой
-- журнал гарантированно сохранён; точка возобновления после сбоя (GLOSSARY).
-- Единственная строка — по одному чекпоинту на слот репликации.

CREATE TABLE IF NOT EXISTS public.checkpoints (
    "name" VARCHAR(255) PRIMARY KEY,
    "last_commit_lsn" BIGINT NOT NULL,
    "updated_at" TIMESTAMPTZ (0) NOT NULL DEFAULT NOW ()
);

COMMENT ON TABLE public.checkpoints IS 'Чекпоинты воркеров: координата в потоке изменений, до которой журнал гарантированно сохранён';

COMMENT ON COLUMN public.checkpoints.name IS 'Имя слота репликации, к которому относится чекпоинт';

COMMENT ON COLUMN public.checkpoints.last_commit_lsn IS 'LSN конца последней транзакции источника, сохранённой в журнал (точка возобновления)';

COMMENT ON COLUMN public.checkpoints.updated_at IS 'Когда чекпоинт был обновлён';
