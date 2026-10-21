-- Реестр источников (ADR-0005, тикет #28): перечень подключений к базам
-- клиентов, управляемый через REST API без перезапуска сервисов.
-- Статус воркера (состояние, время последнего события, текст последней
-- ошибки) пишет воркер — тикет подхвата; API читает его уже здесь.
-- Исполнение удаления (дроп слота и публикации у клиента) — за воркером:
-- DELETE только помечает источник через deleted_at и отвечает 202.

CREATE TABLE IF NOT EXISTS public.sources (
    "name" VARCHAR(54) PRIMARY KEY,
    "url" TEXT NOT NULL,
    "ignore_tables" TEXT[] NOT NULL DEFAULT '{}',
    "message_prefix" VARCHAR(63) NOT NULL DEFAULT '_audit',
    "batch_size" INT NOT NULL DEFAULT 500,
    "worker_state" VARCHAR(63) NOT NULL DEFAULT 'pending',
    "last_event_at" TIMESTAMPTZ (0),
    "last_error" TEXT,
    "deleted_at" TIMESTAMPTZ (0),
    "created_at" TIMESTAMPTZ (0) NOT NULL DEFAULT NOW (),
    "updated_at" TIMESTAMPTZ (0) NOT NULL DEFAULT NOW (),
    -- Правило имён слотов репликации: латиница нижним регистром, цифры,
    -- подчёркивание; слот waltrail_<имя> и публикация audit_<имя>
    -- деривируются из имени (ADR-0005).
    CONSTRAINT sources_name_slot_rule CHECK ("name" ~ '^[a-z0-9_]+$')
);

COMMENT ON TABLE public.sources IS 'Реестр источников журнала: подключения к базам клиентов (ADR-0005)';

COMMENT ON COLUMN public.sources.name IS 'Имя источника: правило имён слотов репликации [a-z0-9_]; слот waltrail_<имя>, публикация audit_<имя>; 54 = 63 (NAMEDATALEN-1) минус префикс waltrail_';

COMMENT ON COLUMN public.sources.url IS 'Строка подключения к базе клиента с кредами, целиком; неизменяема после создания';

COMMENT ON COLUMN public.sources.ignore_tables IS 'Служебные таблицы источника в виде schema.table, не попадающие в журнал';

COMMENT ON COLUMN public.sources.message_prefix IS 'Префикс MESSAGE, интерпретируемого как канал контекста транзакции (ADR-0004); исторический дефолт _audit';

COMMENT ON COLUMN public.sources.batch_size IS 'Размер чанка батчевой вставки строк в журнал';

COMMENT ON COLUMN public.sources.worker_state IS 'Состояние задачи воркера источника; пишет воркер, pending — ждёт подхвата';

COMMENT ON COLUMN public.sources.last_event_at IS 'Когда воркер источника сохранил последнее изменение в журнал';

COMMENT ON COLUMN public.sources.last_error IS 'Текст последней ошибки задачи воркера источника';

COMMENT ON COLUMN public.sources.deleted_at IS 'Когда источник помечен к удалению; NULL — не помечен. Воркер исполняет удаление: дропает слот и публикацию у клиента, снимает строку реестра';

COMMENT ON COLUMN public.sources.created_at IS 'Когда источник зарегистрирован';

COMMENT ON COLUMN public.sources.updated_at IS 'Когда настройки источника менялись последний раз';
