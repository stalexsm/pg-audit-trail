#!/bin/sh

shutdown() {
    echo "Received shutdown signal, stopping processes..."

    # Отправить SIGTERM процессам
    kill -TERM $API_PID $WORKER_APP_PID $WORKER_DEBEZIUM_PID 2>/dev/null

    # Дать время на graceful shutdown (5 секунд)
    sleep 5

    # Если процессы все еще живы, убить принудительно
    kill -KILL $API_PID $WORKER_APP_PID $WORKER_DEBEZIUM_PID 2>/dev/null

    # Ждать завершения
    wait $API_PID $WORKER_APP_PID $WORKER_DEBEZIUM_PID 2>/dev/null

    echo "All processes stopped"
    exit 0
}

trap shutdown TERM INT

echo "Starting processes..."

# Запуск процессов
/app/api & API_PID=$!
/app/worker-app & WORKER_APP_PID=$!
/app/worker-debezium & WORKER_DEBEZIUM_PID=$!

echo "All processes started. PIDs: API=$API_PID, WORKER_APP=$WORKER_APP_PID, WORKER_DEBEZIUM=$WORKER_DEBEZIUM_PID"

# Ждать завершения любого процесса
wait
