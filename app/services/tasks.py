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
    Traduce los roles del Excel a los shortnames técnicos de Moodle.
    Regla del PDF: "Como el excel se basa en profesores sería: editingteacher" como default.
    """
    if pd.isna(role_input) or not str(role_input).strip():
        return "editingteacher" # Default estricto del PDF
    
    val = str(role_input).lower().strip()
    
    mapping = {
        # Español / Términos comunes
        "profesor": "editingteacher",
        "docente": "editingteacher",
        "profesor con permiso": "editingteacher",
        "profesor sin permiso": "teacher",
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
    Procesa el archivo CSV en segundo plano.
    Maneja: Creación de Cursos (con Plantillas), Usuarios, Matriculación y Borrado.
    """
    db = SessionLocal()
    
    try:
        # 1. Crear registro de seguimiento en BD
        upload_rec = FileUpload(
            filename="Carga Masiva (Auto)", 
            operation_type=forced_operation,
            status="PROCESSING"
        )
        db.add(upload_rec)
        db.commit()
        db.refresh(upload_rec)

        # 2. Leer CSV (ya pre-procesado por DataProcessor)
        df = pd.read_csv(io.StringIO(file_content))
        total_records = len(df)
        
        upload_rec.total_records = total_records
        db.commit()
        
        success_count = 0
        error_count = 0

        logger.info(f"Worker iniciando tarea {self.request.id}: {forced_operation} ({total_records} filas)")

        # 3. Bucle de Procesamiento
        for index, row in df.iterrows():
            result = {"success": False, "error": "Operación desconocida"}
            identifier = "Desconocido"
            
            # Convertir fila a dict y limpiar valores nulos de Pandas (NaN -> None)
            row_dict = row.where(pd.notnull(row), None).to_dict()

            try:
                # -------------------------------------------------------
                # A. CREAR USUARIOS
                # -------------------------------------------------------
                if forced_operation == "CREATE_USER":
                    identifier = row_dict.get('username', 'N/A')
                    result = moodle_client.create_user(row_dict)
                
                # -------------------------------------------------------
                # B. MATRICULAR USUARIOS (Con Mapeo de Roles)
                # -------------------------------------------------------
                elif forced_operation == "ENROLL_USER":
                    identifier = f"{row_dict.get('username')} -> {row_dict.get('shortname')}"
                    
                    # Aplicar lógica de roles del PDF
                    raw_role = row_dict.get('role')
                    row_dict['role'] = _map_role_to_technical_name(raw_role)
                    
                    result = moodle_client.enroll_user(row_dict)
                
                # -------------------------------------------------------
                # C. CREAR CURSOS (Con Soporte de Plantillas)
                # -------------------------------------------------------
                elif forced_operation == "CREATE_COURSE":
                    identifier = row_dict.get('shortname', 'N/A')
                    
                    # Paso 1: Crear el curso base
                    result = moodle_client.create_course(row_dict)

                    # Paso 2: Verificar si requiere plantilla (Lógica Problema 4)
                    template_name = row_dict.get("templatecourse")
                    
                    if result.get("success") and template_name and str(template_name).strip():
                        try:
                            # Extraemos el ID del curso recién creado
                            # Moodle devuelve: [{'id': 123, 'shortname': '...'}]
                            data_list = result.get("data", [])
                            if isinstance(data_list, list) and len(data_list) > 0:
                                new_course_id = data_list[0].get("id")
                                
                                logger.info(f"Aplicando plantilla '{template_name}' al curso {new_course_id}...")
                                
                                # Ejecutar importación
                                import_res = moodle_client.import_course_content(new_course_id, template_name)
                                
                                if import_res["success"]:
                                    # Agregamos info al log de éxito
                                    result["data"] = [data_list[0], {"template_status": "Importada OK"}]
                                else:
                                    # Advertencia: Curso creado, pero falló plantilla
                                    warning_msg = f"Curso creado, pero falló plantilla: {import_res.get('error')}"
                                    result["data"] = [data_list[0], {"template_warning": warning_msg}]
                                    logger.warning(warning_msg)
                        
                        except Exception as e:
                            logger.error(f"Error procesando plantilla: {e}")
                            # No marcamos como error fatal porque el curso sí se creó

                # -------------------------------------------------------
                # D. ELIMINAR CURSOS
                # -------------------------------------------------------
                elif forced_operation == "DELETE_COURSE":
                    identifier = row_dict.get('shortname', 'N/A')
                    # Verificar flag 'delete' (debe ser 1)
                    if int(row_dict.get('delete', 0)) == 1:
                        result = moodle_client.delete_course(identifier)
                    else:
                        result = {"success": False, "error": "Flag 'delete' no es 1. Se omitió."}

                # -------------------------------------------------------
                # E. ELIMINAR USUARIOS
                # -------------------------------------------------------
                elif forced_operation == "DELETE_USER":
                    identifier = row_dict.get('username', 'N/A')
                    if int(row_dict.get('delete', 0)) == 1:
                        result = moodle_client.delete_user(identifier)
                    else:
                        result = {"success": False, "error": "Flag 'delete' no es 1. Se omitió."}

                # -------------------------------------------------------
                # F. ACTUALIZAR VISIBILIDAD
                # -------------------------------------------------------
                elif forced_operation == "UPDATE_VISIBILITY":
                    identifier = row_dict.get('shortname', 'N/A')
                    # visible: 1=Mostrar, 0=Ocultar
                    visible_flag = int(row_dict.get('visible', 1)) 
                    result = moodle_client.update_course_visibility(identifier, visible_flag)

                # --- RATE LIMITING (Problema 3) ---
                time.sleep(0.2) 

            except Exception as e:
                # Captura errores internos del código (ej: error parseando int)
                logger.error(f"Error interno fila {index}: {e}")
                result = {"success": False, "error": f"Error Worker: {str(e)}"}

            # --- LOGGING EN BASE DE DATOS ---
            is_success = result.get('success', False)
            status_text = "SUCCESS" if is_success else "ERROR"
            
            # Mensaje detallado (Datos si OK, Error si Falló)
            msg_detail = str(result.get('data')) if is_success else str(result.get('error'))

            log_entry = ProcessingLog(
                upload_id=upload_rec.id,
                identifier=str(identifier)[:255],
                action=forced_operation,
                status=status_text,
                message=msg_detail[:500] 
            )
            db.add(log_entry)

            if is_success:
                success_count += 1
            else:
                error_count += 1

            # Actualizar contadores en DB cada 10 registros
            if index % 10 == 0:
                upload_rec.success_count = success_count
                upload_rec.error_count = error_count
                db.commit()

        # 4. Finalizar Tarea
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
        logger.critical(f"Error FATAL en Celery: {e}")
        if 'upload_rec' in locals():
            upload_rec.status = "FAILED"
            db.commit()
        raise e
    
    finally:
        db.close()