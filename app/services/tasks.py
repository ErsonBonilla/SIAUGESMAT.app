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
celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

celery_app.conf.update(
    task_track_started=True,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='America/Bogota',
    enable_utc=True,
)

def _map_role_to_technical_name(role_input: Any) -> str:
    """
    Traduce los roles del Excel (posiblemente en español) a los shortnames técnicos de Moodle.
    Regla del PDF: "Como el excel se basa en profesores sería: editingteacher" como default.
    """
    if pd.isna(role_input) or not str(role_input).strip():
        return "editingteacher" # Default según PDF
    
    val = str(role_input).lower().strip()
    
    mapping = {
        # Español
        "profesor": "editingteacher",
        "docente": "editingteacher",
        "profesor sin permiso": "teacher",
        "profesor no editor": "teacher",
        "estudiante": "student",
        "alumno": "student",
        "invitado": "guest",
        "gestor": "manager",
        # Inglés / Técnico
        "editingteacher": "editingteacher",
        "teacher": "teacher",
        "student": "student",
        "guest": "guest",
        "manager": "manager",
        "coursecreator": "coursecreator"
    }
    
    return mapping.get(val, "editingteacher") # Fallback seguro

@celery_app.task(bind=True)
def process_moodle_batch(self, file_content: str, forced_operation: str):
    """
    Procesa el archivo línea por línea ejecutando la lógica de negocio y validaciones.
    """
    db = SessionLocal()
    
    try:
        # 1. Registro Inicial
        upload_rec = FileUpload(
            filename="Carga Masiva (Auto)", 
            operation_type=forced_operation,
            status="PROCESSING"
        )
        db.add(upload_rec)
        db.commit()
        db.refresh(upload_rec)

        # 2. Cargar CSV desde memoria (pre-procesado por DataProcessor)
        df = pd.read_csv(io.StringIO(file_content))
        total_records = len(df)
        
        upload_rec.total_records = total_records
        db.commit()
        
        success_count = 0
        error_count = 0

        logger.info(f"Iniciando tarea {self.request.id}: {forced_operation} con {total_records} registros.")

        # 3. Iteración Principal
        for index, row in df.iterrows():
            result = {"success": False, "error": "Operación no soportada"}
            identifier = "Desconocido"
            
            # Convertir row a dict y limpiar nulos
            row_dict = row.where(pd.notnull(row), None).to_dict()

            try:
                # -----------------------------------------------------------
                # A. CREACIÓN DE USUARIOS
                # -----------------------------------------------------------
                if forced_operation == "CREATE_USER":
                    identifier = row_dict.get('username', 'N/A')
                    result = moodle_client.create_user(row_dict)
                
                # -----------------------------------------------------------
                # B. MATRICULACIÓN (Gestión de Roles Estricta)
                # -----------------------------------------------------------
                elif forced_operation == "ENROLL_USER":
                    identifier = f"{row_dict.get('username')} -> {row_dict.get('shortname')}"
                    
                    # Aplicar lógica de roles del PDF
                    raw_role = row_dict.get('role')
                    technical_role = _map_role_to_technical_name(raw_role)
                    row_dict['role'] = technical_role
                    
                    result = moodle_client.enroll_user(row_dict)
                
                # -----------------------------------------------------------
                # C. CREACIÓN DE CURSOS
                # -----------------------------------------------------------
                elif forced_operation == "CREATE_COURSE":
                    identifier = row_dict.get('shortname', 'N/A')
                    result = moodle_client.create_course(row_dict)

                # -----------------------------------------------------------
                # D. ELIMINACIÓN DE CURSOS [NUEVO]
                # -----------------------------------------------------------
                elif forced_operation == "DELETE_COURSE":
                    identifier = row_dict.get('shortname', 'N/A')
                    # Verificar flag 'delete' si viene en el Excel
                    should_delete = int(row_dict.get('delete', 0))
                    
                    if should_delete == 1:
                        result = moodle_client.delete_course(identifier)
                    else:
                        result = {"success": False, "error": "Flag 'delete' no es 1. Se omitió."}

                # -----------------------------------------------------------
                # E. VISIBILIDAD DE CURSOS [NUEVO]
                # -----------------------------------------------------------
                elif forced_operation == "UPDATE_VISIBILITY":
                    identifier = row_dict.get('shortname', 'N/A')
                    visible_flag = int(row_dict.get('visible', 1)) # Default 1 (Visible)
                    result = moodle_client.update_course_visibility(identifier, visible_flag)

                # -----------------------------------------------------------
                # F. ELIMINACIÓN DE USUARIOS [NUEVO]
                # -----------------------------------------------------------
                elif forced_operation == "DELETE_USER":
                    identifier = row_dict.get('username', 'N/A')
                    should_delete = int(row_dict.get('delete', 0))
                    
                    if should_delete == 1:
                        result = moodle_client.delete_user(identifier)
                    else:
                        result = {"success": False, "error": "Flag 'delete' no es 1. Se omitió."}

                # Rate Limiting: 5 peticiones/segundo máx para estabilidad
                time.sleep(0.2)

            except Exception as e:
                logger.error(f"Excepción interna fila {index}: {e}")
                result = {"success": False, "error": f"Error Worker: {str(e)}"}

            # -----------------------------------------------------------
            # LOGGING Y AUDITORÍA
            # -----------------------------------------------------------
            is_success = result.get('success', False)
            status_text = "SUCCESS" if is_success else "ERROR"
            
            # Priorizamos mostrar el mensaje de error si falló, o la data si tuvo éxito
            msg_detail = str(result.get('error')) if not is_success else str(result.get('data'))

            log_entry = ProcessingLog(
                upload_id=upload_rec.id,
                identifier=str(identifier)[:255], # Truncar por seguridad de DB
                action=forced_operation,
                status=status_text,
                message=msg_detail[:500] 
            )
            db.add(log_entry)

            if is_success:
                success_count += 1
            else:
                error_count += 1

            # Actualización periódica en DB (cada 10 filas)
            if index % 10 == 0:
                upload_rec.success_count = success_count
                upload_rec.error_count = error_count
                db.commit()

        # 4. Cierre de Tarea
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
        db.rollback()
        logger.critical(f"Error FATAL en tarea Celery: {e}")
        if 'upload_rec' in locals():
            upload_rec.status = "FAILED"
            db.commit()
        raise e
    
    finally:
        db.close()