-- Add down migration script here
DROP INDEX IF EXISTS audits_primary_key_index;

ALTER TABLE public.audits
DROP COLUMN IF EXISTS primary_key;
