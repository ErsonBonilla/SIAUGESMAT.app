#!/bin/bash

# Detener el script si hay un error
set -e

# --- FUNCIONES DE ESPERA ---
# Espera a que la base de datos esté lista para recibir conexiones
wait_for_db() {
  echo "⏳ Esperando a PostgreSQL en $DB_HOST:$DB_PORT..."
  while ! nc -z $DB_HOST $DB_PORT; do
    sleep 1
  done
  echo "✅ PostgreSQL está listo."
}

# Espera a que Redis esté listo
wait_for_redis() {
  echo "⏳ Esperando a Redis en $REDIS_HOST:6379..."
  while ! nc -z $REDIS_HOST 6379; do
    sleep 1
  done
  echo "✅ Redis está listo."
}

# --- LÓGICA DE INICIO SEGÚN EL ROL ---

if [ "$CONTAINER_ROLE" = "web" ]; then
    # ROL: SERVIDOR WEB (FastAPI + NiceGUI)
    wait_for_db
    wait_for_redis
    echo "🚀 Iniciando Servidor Web SIAUGESMAT..."
    # Ejecutamos con Uvicorn para alto rendimiento
    exec uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 4

elif [ "$CONTAINER_ROLE" = "worker" ]; then
    # ROL: PROCESADOR DE TAREAS (Celery)
    wait_for_redis
    echo "⚙️ Iniciando Celery Worker..."
    # Ejecutamos el worker escuchando la cola de tareas de Moodle
    exec celery -A app.services.tasks.celery_app worker --loglevel=info --concurrency=2

elif [ "$CONTAINER_ROLE" = "scheduler" ]; then
    # ROL: TAREAS PROGRAMADAS (Celery Beat - Opcional)
    wait_for_redis
    echo "📅 Iniciando Celery Beat..."
    exec celery -A app.services.tasks.celery_app beat --loglevel=info

else
    echo "❌ Error: La variable CONTAINER_ROLE ('$CONTAINER_ROLE') no es válida."
    echo "Debe ser: 'web', 'worker' o 'scheduler'."
    exit 1
fi