-- Инициализация источника CDC (dev-стенд): подготовка к логической репликации.
-- Выполняется автоматически при первом создании volume (docker-entrypoint-initdb.d).

-- Публикация всех таблиц: новый воркер CDC читает изменения через pgoutput.
-- wal_level=logical задаётся командой запуска контейнера (см. compose.yaml).
CREATE PUBLICATION audit FOR ALL TABLES;

-- Слот репликации для dev по умолчанию НЕ создаётся автоматически: пока воркера CDC нет,
-- пустой слот только удерживает WAL. Создать вручную, когда понадобится:
--
--   SELECT pg_create_logical_replication_slot('waltrail', 'pgoutput');
--
-- Удалить (например, после пересоздания стенда):
--
--   SELECT pg_drop_replication_slot('waltrail');
