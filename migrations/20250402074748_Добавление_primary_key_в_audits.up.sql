-- Add up migration script here
ALTER TABLE public.audits
ADD COLUMN primary_key VARCHAR(255) NULL;

CREATE INDEX audits_primary_key_index ON public.audits (primary_key);

COMMENT ON COLUMN public.audits.primary_key IS 'Уникальный идентификатор измененной записи (опционально)';
