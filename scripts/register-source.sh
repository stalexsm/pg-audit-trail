#!/usr/bin/env bash
# Идемпотентная регистрация источника журнала на поднятый стенд Waltrail.
#
# Повторный запуск безопасен: если источник с таким именем уже зарегистрирован
# с той же строкой подключения — скрипт завершается успехом; с другой строкой —
# ошибкой (URL источника неизменяем после создания, ADR-0005: переезд клиента
# на другой сервер — через DELETE /api/sources/<имя> и повторную регистрацию).
#
# Использование:
#   scripts/register-source.sh [ИМЯ_ИСТОЧНИКА] [URL_ИСТОЧНИКА]
#
# Переменные окружения:
#   WALTRAIL_API_URL    базовый URL API (по умолчанию http://localhost:8000)
#   WALTRAIL_SOURCE_URL строка подключения источника по умолчанию
#
# По умолчанию регистрирует источник demo на db-src демо-стенда: URL адресует
# базу по сети compose (db-src:5432) — по этому URL к клиенту ходит воркер.
#
# Зависимости: curl, python3 (разбор JSON-ответов API).
set -euo pipefail

API_URL="${WALTRAIL_API_URL:-http://localhost:8000}"
SOURCE_NAME="${1:-demo}"
SOURCE_URL="${2:-${WALTRAIL_SOURCE_URL:-postgresql://postgres:postgres@db-src:5432/postgres}}"

# Ожидание API после compose up: сервисы применяют миграции на старте.
WAIT_ATTEMPTS=30
WAIT_INTERVAL_SECS=2

# Печатает шапку файла (строки-комментарии после шебанга) — это текст --help:
# правки шапки не требуют правки этой функции (длина не зафиксирована).
usage() {
    awk 'NR > 1 && !/^#/ { exit } NR > 1 { print substr($0, 3) }' "${BASH_SOURCE[0]}"
}

connection_lost() {
    echo "Ошибка: соединение с API потеряно ($API_URL)" >&2
    exit 1
}

# Печатает значение поля плоского JSON-объекта (пусто — поля нет или null).
json_field() {
    python3 - "$1" "$2" <<'PY'
import json, sys

try:
    value = json.loads(sys.argv[1]).get(sys.argv[2])
except json.JSONDecodeError:
    value = None
print("" if value is None else value)
PY
}

# GET/POST к API: тело в BODY, код ответа в STATUS; ошибка соединения — 0.
http_request() {
    local method="$1" path="$2" body="${3:-}"
    local payload=(-sS -w $'\n%{http_code}' -X "$method" "$API_URL$path")
    [ -n "$body" ] && payload+=(-H 'content-type: application/json' -d "$body")

    local response
    if ! response=$(curl "${payload[@]}" 2>/dev/null); then
        STATUS=0
        BODY=""
        return
    fi
    STATUS="${response##*$'\n'}"
    BODY="${response%$'\n'*}"
}

wait_for_api() {
    local attempt
    for ((attempt = 1; attempt <= WAIT_ATTEMPTS; attempt++)); do
        http_request GET /health
        [ "$STATUS" = "200" ] && return 0
        sleep "$WAIT_INTERVAL_SECS"
    done
    echo "Ошибка: API не отвечает на $API_URL/health после $((WAIT_ATTEMPTS * WAIT_INTERVAL_SECS)) с" >&2
    return 1
}

# Сверяет зарегистрированный источник с ожидаемым: успех — URL совпал,
# ошибка — имя занято другой строкой подключения (URL неизменяем, ADR-0005).
check_existing() {
    local registered_url
    registered_url=$(json_field "$BODY" url)

    if [ "$registered_url" = "$SOURCE_URL" ]; then
        echo "Источник '$SOURCE_NAME' уже зарегистрирован — ок (идемпотентный запуск)"
        return 0
    fi
    echo "Ошибка: имя '$SOURCE_NAME' занято источником с другой строкой подключения" >&2
    echo "       зарегистрировано: $registered_url" >&2
    echo "       ожидается:        $SOURCE_URL" >&2
    echo "URL источника неизменяем: удалите источник (DELETE $API_URL/api/sources/$SOURCE_NAME)" >&2
    echo "и повторите регистрацию (строки журнала удалённого источника сохранятся)" >&2
    return 1
}

# Тело регистрации: JSON собирает python3 — экранирование кавычек и слэшей
# в имени и URL не зависит от их содержимого.
json_body() {
    python3 - "$SOURCE_NAME" "$SOURCE_URL" <<'PY'
import json, sys

print(json.dumps({"name": sys.argv[1], "url": sys.argv[2]}))
PY
}

case "${1:-}" in
    -h | --help)
        usage
        exit 0
        ;;
esac

wait_for_api

# Источник уже в реестре? (404 — нет, регистрируем; иное — ошибка API)
http_request GET "/api/sources/$SOURCE_NAME"
case "$STATUS" in
    200) check_existing ;;
    404) ;;
    0) connection_lost ;;
    *)
        echo "Ошибка: GET /api/sources/$SOURCE_NAME ответил $STATUS: $BODY" >&2
        exit 1
        ;;
esac

if [ "${STATUS}" != "404" ]; then
    exit 0
fi

# Регистрация: 201 — успех; 400 с дубликатом имени — гонка с параллельным
# регистратором, перепроверяем реестр.
http_request POST /api/sources "$(json_body)"
case "$STATUS" in
    201)
        echo "Источник '$SOURCE_NAME' зарегистрирован: $SOURCE_URL"
        echo "Воркер подхватит его без перезапуска: создаст у клиента публикацию"
        echo "audit_$SOURCE_NAME и слот waltrail_$SOURCE_NAME (статус: GET $API_URL/api/sources/$SOURCE_NAME)"
        ;;
    400)
        if grep -q 'уже занято' <<<"$BODY"; then
            http_request GET "/api/sources/$SOURCE_NAME"
            check_existing
        else
            echo "Ошибка: регистрация отклонена (400): $BODY" >&2
            exit 1
        fi
        ;;
    0) connection_lost ;;
    *)
        echo "Ошибка: POST /api/sources ответил $STATUS: $BODY" >&2
        exit 1
        ;;
esac
