#!/bin/bash

# Detener el script inmediatamente si ocurre un error
set -e

# --- FUNCIONES DE ESPERA (HEALTH CHECKS) ---

wait_for_db() {
  echo "⏳ Esperando a PostgreSQL en $DB_HOST:$DB_PORT..."
  # CORRECCIÓN: Se añade -w 1 para evitar bloqueos si el firewall descarta paquetes
  while ! nc -z -w 1 $DB_HOST $DB_PORT; do
    echo "   ... Base de datos no disponible, reintentando en 1s"
    sleep 1
  done
  echo "✅ PostgreSQL está listo y aceptando conexiones."
}

wait_for_redis() {
  echo "⏳ Esperando a Redis en $REDIS_HOST:6379..."
  # CORRECCIÓN: Se añade -w 1 por seguridad
  while ! nc -z -w 1 $REDIS_HOST 6379; do
    echo "   ... Redis no disponible, reintentando en 1s"
    sleep 1
  done
  echo "✅ Redis está listo."
}

# --- LÓGICA DE INICIO SEGÚN EL ROL ---

echo "🔧 Configurando entorno para rol: $CONTAINER_ROLE"

if [ "$CONTAINER_ROLE" = "web" ]; then
    # ROL: SERVIDOR WEB (FastAPI + NiceGUI)
    wait_for_db
    wait_for_redis
    
    echo "🚀 Iniciando Servidor Web SIAUGESMAT (Uvicorn)..."
    # Ejecutamos con Uvicorn.
    # --host 0.0.0.0 es vital para que Docker exponga el puerto.
    exec uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 4

elif [ "$CONTAINER_ROLE" = "worker" ]; then
    # ROL: PROCESADOR DE TAREAS (Celery)
    wait_for_redis
    # Nota: El worker no necesita esperar estrictamente a la DB aquí, 
    # ya que la conexión se abre por tarea, pero es buena práctica esperar.
    wait_for_db 
    
    echo "⚙️ Iniciando Celery Worker..."
    # Ejecutamos el worker escuchando la cola de tareas
    # -E habilita eventos (útil para monitoreo)
    exec celery -A app.services.tasks.celery_app worker --loglevel=info --concurrency=2 -E

elif [ "$CONTAINER_ROLE" = "scheduler" ]; then
    # ROL: TAREAS PROGRAMADAS (Celery Beat - Opcional)
    wait_for_redis
    echo "📅 Iniciando Celery Beat (Scheduler)..."
    exec celery -A app.services.tasks.celery_app beat --loglevel=info

else
    echo "❌ Error Crítico: La variable CONTAINER_ROLE ('$CONTAINER_ROLE') no es válida."
    echo "Valores permitidos: 'web', 'worker', 'scheduler'."
    exit 1
fi