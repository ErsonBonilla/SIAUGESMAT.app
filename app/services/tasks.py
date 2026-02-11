from celery import Celery
import pandas as pd
import io
import time
import logging
from typing import Dict, Any

# Importaciones del proyecto
from app.core.config import settings
from app.db.session import SessionLocal
from app.models.models import FileUpload, ProcessingLog
from app.services.moodle_sync import moodle_client

# Configuración del Logger
logger = logging.getLogger(__name__)

# Configuración de Celery
# Definimos el nombre 'worker' y la conexión a Redis
celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

# Configuraciones opcionales de Celery para robustez
celery_app.conf.update(
    task_track_started=True,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='America/Bogota',
    enable_utc=True,
)

@celery_app.task(bind=True)
def process_moodle_batch(self, file_content: str, forced_operation: str):
    """
    Procesa el archivo CSV en segundo plano, fila por fila.
    Incluye control de tasa (Rate Limiting) para no saturar Moodle.
    """
    db = SessionLocal()
    
    try:
        # 1. Crear registro inicial de carga en la Base de Datos
        # Esto permite al usuario ver que el archivo está "PROCESANDO"
        upload_rec = FileUpload(
            filename="Carga Masiva (Auto)", 
            operation_type=forced_operation,
            status="PROCESSING"
        )
        db.add(upload_rec)
        db.commit()
        db.refresh(upload_rec)

        # 2. Leer datos del CSV (que viene como string desde Redis)
        df = pd.read_csv(io.StringIO(file_content))
        total_records = len(df)
        
        # Actualizamos el total de registros detectados
        upload_rec.total_records = total_records
        db.commit()
        
        success_count = 0
        error_count = 0

        logger.info(f"Iniciando tarea {self.request.id}: {forced_operation} con {total_records} registros.")

        # 3. Iterar y ejecutar contra la API de Moodle
        for index, row in df.iterrows():
            result = {"success": False, "error": "Operación desconocida"}
            identifier = "Desconocido"
            row_dict = row.to_dict()

            try:
                # --- LÓGICA DE NEGOCIO ---
                if forced_operation == "CREATE_USER":
                    identifier = row_dict.get('username', 'N/A')
                    result = moodle_client.create_user(row_dict)
                
                elif forced_operation == "ENROLL_USER":
                    identifier = f"{row_dict.get('username')} -> {row_dict.get('shortname')}"
                    result = moodle_client.enroll_user(row_dict)
                
                elif forced_operation == "CREATE_COURSE":
                    identifier = row_dict.get('shortname', 'N/A')
                    result = moodle_client.create_course(row_dict)

                # --- CONTROL DE TASA (RATE LIMITING - SOLUCIÓN PROBLEMA 3) ---
                # Esperamos 0.2 segundos entre peticiones.
                # Esto previene errores HTTP 503 (Service Unavailable) en Moodle.
                time.sleep(0.2)

            except Exception as e:
                # Capturamos errores de código (no de API) para que el bucle continúe
                logger.error(f"Excepción interna procesando fila {index}: {e}")
                result = {"success": False, "error": f"Error interno del worker: {str(e)}"}

            # --- REGISTRO DE AUDITORÍA (LOGS) ---
            is_success = result.get('success', False)
            status_text = "SUCCESS" if is_success else "ERROR"
            
            # Mensaje detallado: Si es éxito, mostramos la data, si no, el error
            msg_detail = str(result.get('data')) if is_success else str(result.get('error'))

            # Guardar log individual en DB
            log_entry = ProcessingLog(
                upload_id=upload_rec.id,
                identifier=str(identifier),
                action=forced_operation,
                status=status_text,
                message=msg_detail[:500] # Truncamos mensajes muy largos por seguridad
            )
            db.add(log_entry)

            # Contadores
            if is_success:
                success_count += 1
            else:
                error_count += 1

            # Opcional: Actualizar progreso en DB cada 10 registros para no sobrecargar la DB
            if index % 10 == 0:
                upload_rec.success_count = success_count
                upload_rec.error_count = error_count
                db.commit()

        # 4. Finalización de la Tarea
        upload_rec.status = "COMPLETED"
        upload_rec.success_count = success_count
        upload_rec.error_count = error_count
        db.commit()
        
        logger.info(f"Tarea finalizada. Éxitos: {success_count}, Errores: {error_count}")
        return {
            "status": "completed", 
            "total": total_records, 
            "success": success_count, 
            "errors": error_count
        }

    except Exception as e:
        # Manejo de error catastrófico (ej: fallo de conexión a DB general)
        db.rollback()
        logger.critical(f"Error fatal en la tarea Celery: {e}")
        
        if 'upload_rec' in locals():
            upload_rec.status = "FAILED"
            upload_rec.error_count = total_records if 'total_records' in locals() else 0
            db.commit()
            
        raise e # Re-lanzar para que Celery marque la tarea como Failed
    
    finally:
        db.close()