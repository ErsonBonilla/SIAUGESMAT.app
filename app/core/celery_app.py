import os
from celery import Celery

# 1. Configuración de URLs de conexión (Broker y Backend)
# Utilizamos os.getenv para leer del .env o de Docker, con valores por defecto para local.
# Nota: 'redis' es el nombre del servicio definido en docker-compose.yml
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

BROKER_URL = os.getenv("CELERY_BROKER_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")

# 2. Inicialización de la instancia Celery
celery_app = Celery(
    "siaugesmat",
    broker=BROKER_URL,
    backend=BACKEND_URL
)

# 3. Configuración del comportamiento (Settings)
celery_app.conf.update(
    # Formato de serialización: JSON es seguro y estándar.
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    
    # Zona horaria: Importante para la Universidad del Tolima (Colombia)
    timezone="America/Bogota",
    enable_utc=True,
    
    # Robustez: Si el worker se muere a mitad de tarea, no perder el mensaje.
    task_acks_late=True,
    
    # Dónde buscar las tareas (Critical Step)
    # Aquí es donde conectamos el "Motor" con la "Lógica"
    imports=["app.services.tasks"]
)

# Configuración opcional de rutas (si quisieras colas separadas para tareas lentas)
# celery_app.conf.task_routes = {
#     "app.services.tasks.process_moodle_batch": {"queue": "moodle_bulk_ops"}
# }