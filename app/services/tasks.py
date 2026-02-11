from celery import Celery
import pandas as pd
import io
import time
import logging
from typing import Dict, Any

from app.core.config import settings
from app.db.session import SessionLocal
from app.models.models import FileUpload, ProcessingLog
from app.services.moodle_sync import moodle_client

# Configuración del Logger
logger = logging.getLogger(__name__)

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
    if pd.isna(role_input) or not str(role_input).strip():
        return "editingteacher" 
    
    val = str(role_input).lower().strip()
    mapping = {
        "profesor": "editingteacher",
        "docente": "editingteacher",
        "profesor con permiso": "editingteacher",
        "profesor sin permiso": "teacher",
        "estudiante": "student",
        "alumno": "student",
        "invitado": "guest",
        "gestor": "manager",
        "editingteacher": "editingteacher",
        "teacher": "teacher",
        "student": "student",
        "guest": "guest",
        "manager": "manager",
        "coursecreator": "coursecreator"
    }
    return mapping.get(val, "editingteacher")

@celery_app.task(bind=True)
def process_moodle_batch(self, file_content: str, forced_operation: str):
    db = SessionLocal()
    
    try:
        # 1. Crear registro maestro de la carga
        upload_rec = FileUpload(
            filename="Carga Masiva (Auto)", 
            operation_type=forced_operation,
            status="PROCESSING"
        )
        db.add(upload_rec)
        db.commit()
        db.refresh(upload_rec)

        df = pd.read_csv(io.StringIO(file_content))
        total_records = len(df)
        
        upload_rec.total_records = total_records
        db.commit()
        
        success_count = 0
        error_count = 0

        logger.info(f"Worker iniciando tarea {self.request.id}: {forced_operation} ({total_records} filas)")

        # --- INICIO DE REFACTORIZACIÓN BULK INSERT ---
        logs_batch = []
        BATCH_SIZE = 100 # Procesar y guardar de a 100 registros en la BD

        for index, row in df.iterrows():
            result = {"success": False, "error": "Operación desconocida"}
            identifier = "Desconocido"
            
            row_dict = row.where(pd.notnull(row), None).to_dict()

            try:
                # A. CREAR USUARIOS
                if forced_operation == "CREATE_USER":
                    identifier = row_dict.get('username', 'N/A')
                    result = moodle_client.create_user(row_dict)
                
                # B. MATRICULAR USUARIOS
                elif forced_operation == "ENROLL_USER":
                    identifier = f"{row_dict.get('username')} -> {row_dict.get('shortname')}"
                    row_dict['role'] = _map_role_to_technical_name(row_dict.get('role'))
                    result = moodle_client.enroll_user(row_dict)
                
                # C. CREAR CURSOS
                elif forced_operation == "CREATE_COURSE":
                    identifier = row_dict.get('shortname', 'N/A')
                    result = moodle_client.create_course(row_dict)

                    template_name = row_dict.get("templatecourse")
                    
                    if result.get("success") and template_name and str(template_name).strip():
                        try:
                            data_list = result.get("data", [])
                            if isinstance(data_list, list) and len(data_list) > 0:
                                new_course_id = data_list[0].get("id")
                                logger.info(f"Aplicando plantilla '{template_name}' al curso {new_course_id}...")
                                
                                import_res = moodle_client.import_course_content(new_course_id, template_name)
                                
                                if not import_res["success"] and template_name != "FC2025A":
                                    logger.warning(f"Plantilla '{template_name}' falló. Intentando con fallback 'FC2025A'...")
                                    import_res = moodle_client.import_course_content(new_course_id, "FC2025A")
                                    
                                    if import_res["success"]:
                                        result["data"] = [data_list[0], {"template_status": "Fallback FC2025A Aplicado"}]
                                    else:
                                        result["data"] = [data_list[0], {"template_warning": f"Fallaron ambas plantillas: {import_res.get('error')}"}]
                                elif import_res["success"]:
                                    result["data"] = [data_list[0], {"template_status": "Importada OK"}]
                                else:
                                    result["data"] = [data_list[0], {"template_warning": f"Falló plantilla: {import_res.get('error')}"}]
                        
                        except Exception as e:
                            logger.error(f"Error procesando plantilla: {e}")

                # D, E, F: ELIMINAR CURSOS, ELIMINAR USUARIOS, ACTUALIZAR VISIBILIDAD...
                elif forced_operation == "DELETE_COURSE":
                    identifier = row_dict.get('shortname', 'N/A')
                    result = moodle_client.delete_course(identifier) if int(row_dict.get('delete', 0)) == 1 else {"success": False, "error": "Flag 'delete' no es 1"}

                elif forced_operation == "DELETE_USER":
                    identifier = row_dict.get('username', 'N/A')
                    result = moodle_client.delete_user(identifier) if int(row_dict.get('delete', 0)) == 1 else {"success": False, "error": "Flag 'delete' no es 1"}

                elif forced_operation == "UPDATE_VISIBILITY":
                    identifier = row_dict.get('shortname', 'N/A')
                    result = moodle_client.update_course_visibility(identifier, int(row_dict.get('visible', 1)))

                time.sleep(0.2) 

            except Exception as e:
                logger.error(f"Error interno fila {index}: {e}")
                result = {"success": False, "error": f"Error Worker: {str(e)}"}

            # --- PREPARACIÓN DEL LOG ---
            is_success = result.get('success', False)
            
            log_entry = ProcessingLog(
                upload_id=upload_rec.id,
                identifier=str(identifier)[:255],
                action=forced_operation,
                status="SUCCESS" if is_success else "ERROR",
                message=str(result.get('data'))[:500] if is_success else str(result.get('error'))[:500]
            )
            
            # Agregamos el log a la lista en memoria (Aún NO toca la base de datos)
            logs_batch.append(log_entry)

            if is_success:
                success_count += 1
            else:
                error_count += 1

            # --- INSERCIÓN MASIVA (BULK) CADA 100 REGISTROS ---
            if len(logs_batch) >= BATCH_SIZE:
                db.bulk_save_objects(logs_batch) # ¡Un solo INSERT de 100 filas!
                upload_rec.success_count = success_count
                upload_rec.error_count = error_count
                db.commit()
                logs_batch.clear() # Vaciamos la lista para los siguientes 100

        # --- INSERTAR EL REMANENTE AL FINALIZAR EL FOR ---
        # (Ej: Si eran 150 filas, aquí se insertan las últimas 50)
        if logs_batch:
            db.bulk_save_objects(logs_batch)
            upload_rec.success_count = success_count
            upload_rec.error_count = error_count
            db.commit()

        # Finalizar Tarea
        upload_rec.status = "COMPLETED"
        db.commit()
        
        logger.info(f"Tarea finalizada. Éxitos: {success_count}, Errores: {error_count}")
        return {"status": "completed", "total": total_records, "success": success_count, "errors": error_count}

    except Exception as e:
        db.rollback()
        logger.critical(f"Error FATAL en Celery: {e}")
        if 'upload_rec' in locals():
            upload_rec.status = "FAILED"
            db.commit()
        raise e
    
    finally:
        db.close()