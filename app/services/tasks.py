# app/services/tasks.py
from celery import Celery
from app.core.config import settings
from app.db.session import SessionLocal
from app.models.models import FileUpload, ProcessingLog
from app.services.moodle_sync import moodle_client
import pandas as pd
import io
import logging

# Configuración de Celery
celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

logger = logging.getLogger(__name__)

@celery_app.task(bind=True)
def process_moodle_batch(self, file_content: str, forced_operation: str):
    """
    Procesa el CSV en segundo plano.
    file_content: CSV en formato string.
    """
    db = SessionLocal()
    try:
        # 1. Crear registro de carga
        upload_rec = FileUpload(
            filename="auto_upload.csv", # Podrías pasar el nombre real como argumento
            operation_type=forced_operation,
            status="PROCESSING"
        )
        db.add(upload_rec)
        db.commit()

        # 2. Leer datos
        df = pd.read_csv(io.StringIO(file_content))
        total = len(df)
        upload_rec.total_records = total
        
        success_count = 0
        error_count = 0

        # 3. Iterar y ejecutar contra Moodle
        for index, row in df.iterrows():
            result = {"success": False, "error": "Operación no soportada"}
            identifier = "Desconocido"

            try:
                # Mapeo de operaciones
                if forced_operation == "CREATE_USER":
                    identifier = row.get('username', 'N/A')
                    result = moodle_client.create_user(row.to_dict())
                
                elif forced_operation == "ENROLL_USER":
                    identifier = f"{row.get('username')} -> {row.get('shortname')}"
                    result = moodle_client.enroll_user(row.to_dict())
                
                elif forced_operation == "CREATE_COURSE":
                    identifier = row.get('shortname', 'N/A')
                    result = moodle_client.create_course(row.to_dict())

                # Registro de Log
                status = "SUCCESS" if result.get('success') else "ERROR"
                msg = str(result.get('data') if result.get('success') else result.get('error'))

                log = ProcessingLog(
                    upload_id=upload_rec.id,
                    identifier=identifier,
                    action=forced_operation,
                    status=status,
                    message=msg
                )
                db.add(log)

                if status == "SUCCESS":
                    success_count += 1
                else:
                    error_count += 1

            except Exception as e:
                error_count += 1
                logger.error(f"Error en fila {index}: {e}")
        
        # 4. Actualizar estado final
        upload_rec.status = "COMPLETED"
        upload_rec.success_count = success_count
        upload_rec.error_count = error_count
        db.commit()
        
        return {"status": "completed", "processed": total}

    except Exception as e:
        db.rollback()
        logger.error(f"Error fatal en tarea: {e}")
        if 'upload_rec' in locals():
            upload_rec.status = "FAILED"
            db.commit()
        raise e
    finally:
        db.close()