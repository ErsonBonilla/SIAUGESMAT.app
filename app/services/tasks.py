import pandas as pd
import io
import logging
from typing import Optional
from app.core.celery_app import celery_app
from app.services.moodle_sync import moodle_client

# Configuración del Logger para Celery
logger = logging.getLogger(__name__)

@celery_app.task(bind=True, name="process_moodle_batch")
def process_moodle_batch(self, csv_content: str, forced_operation: Optional[str] = None):
    """
    Tarea asincrónica que procesa un lote de operaciones desde un CSV.
    Detecta automáticamente el tipo de operación basándose en las columnas,
    a menos que se fuerce una operación específica.
    """
    try:
        # Convertir CSV string a DataFrame
        df = pd.read_csv(io.StringIO(csv_content))
        total_rows = len(df)
        logger.info(f"Iniciando procesamiento de {total_rows} registros.")
        
        results = {
            "success": 0,
            "failed": 0,
            "errors": []
        }

        # Iterar sobre cada fila del archivo
        for index, row in df.iterrows():
            try:
                # Actualizar estado de la tarea (para barra de progreso en UI)
                self.update_state(state='PROGRESS', meta={
                    'current': index + 1,
                    'total': total_rows,
                    'status': f'Procesando fila {index + 1}...'
                })

                # --- LÓGICA DE DETECCIÓN DE OPERACIÓN ---
                
                # 1. ELIMINAR CURSO (Columna 'delete' y 'shortname')
                if 'delete' in row and 'shortname' in row and row['delete'] == 1:
                    # MoodleClient necesita un método delete_course (agregarlo si falta)
                    # moodle_client.delete_course(row['shortname'])
                    logger.info(f"Eliminando curso: {row['shortname']}")
                
                # 2. ELIMINAR USUARIO (Columna 'delete' y 'username')
                elif 'delete' in row and 'username' in row and row['delete'] == 1:
                    # moodle_client.delete_user(row['username'])
                    logger.info(f"Eliminando usuario: {row['username']}")

                # 3. CAMBIAR VISIBILIDAD (Columna 'visible' y 'shortname')
                elif 'visible' in row and 'shortname' in row:
                    moodle_client.update_course_visibility(
                        shortname=row['shortname'], 
                        visible=int(row['visible'])
                    )

                # 4. MATRICULAR USUARIO (Columnas 'username', 'role1', 'shortname' o 'course1')
                # Nota: 'course1' es estándar en CSV Moodle, pero nuestro procesador usa 'shortname'
                elif 'username' in row and ('shortname' in row or 'course1' in row):
                    course_ref = row.get('shortname', row.get('course1'))
                    role_ref = row.get('role1', 'student')
                    
                    moodle_client.enroll_user(
                        shortname_course=course_ref,
                        username=row['username'],
                        role_shortname=role_ref
                    )

                # 5. CREAR CURSO (Default si tiene 'fullname' y 'shortname')
                elif 'fullname' in row and 'shortname' in row:
                    # Convertir la fila a diccionario para pasarla al cliente
                    course_data = row.to_dict()
                    moodle_client.create_course(course_data)

                else:
                    logger.warning(f"Fila {index}: No se pudo determinar la operación.")
                    results["failed"] += 1
                    continue

                results["success"] += 1

            except Exception as e:
                logger.error(f"Error en fila {index}: {str(e)}")
                results["failed"] += 1
                results["errors"].append(f"Fila {index}: {str(e)}")
        
        # Resultado final
        return {
            "status": "COMPLETED",
            "processed": total_rows,
            "success": results["success"],
            "failed": results["failed"],
            "errors": results["errors"]  # Opcional: devolver errores detallados
        }

    except Exception as e:
        logger.critical(f"Error fatal procesando el lote: {str(e)}")
        return {"status": "FAILED", "error": str(e)}