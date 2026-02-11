from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from celery.result import AsyncResult

from app.core.deps import get_db
from app.services.tasks import process_moodle_batch
from app.services.data_processor import processor
from typing import Dict, Any

# Crear el Router
router = APIRouter()

# ---------------------------------------------------------
# 1. HEALTH CHECK (Crítico para Kubernetes)
# ---------------------------------------------------------
@router.get("/health", status_code=status.HTTP_200_OK)
def health_check(db: Session = Depends(get_db)):
    """
    Endpoint utilizado por Kubernetes 'livenessProbe'.
    Verifica que la API responda y que la Base de Datos esté conectada.
    """
    try:
        # Ejecuta una consulta simple para verificar conexión a DB
        db.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected", "version": "1.0.0"}
    except Exception as e:
        # Si falla la DB, devolvemos 503 para que K8s sepa que algo va mal
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection failed: {str(e)}"
        )

# ---------------------------------------------------------
# 2. ESTADO DE TAREAS (Para la UI y Polling)
# ---------------------------------------------------------
@router.get("/task/{task_id}", response_model=Dict[str, Any])
def get_task_status(task_id: str):
    """
    Consulta el estado de una tarea de Celery (Redis).
    Estados posibles: PENDING, STARTED, SUCCESS, FAILURE, RETRY.
    """
    task_result = AsyncResult(task_id)
    
    response = {
        "task_id": task_id,
        "status": task_result.status,
        "result": None
    }

    if task_result.status == "SUCCESS":
        response["result"] = task_result.result
    elif task_result.status == "FAILURE":
        response["error"] = str(task_result.result)

    return response

# ---------------------------------------------------------
# 3. ENDPOINT DE CARGA API (Headless / Programático)
# ---------------------------------------------------------
@router.post("/upload", status_code=status.HTTP_201_CREATED)
async def upload_file_api(
    file: UploadFile = File(...), 
    background_tasks: bool = True
):
    """
    Permite subir archivos vía CURL o Postman sin usar la interfaz gráfica.
    Útil para integraciones automáticas desde otros sistemas de la UT.
    """
    if not file.filename.endswith(('.xls', '.xlsx')):
        raise HTTPException(400, "Formato no válido. Use .xlsx")

    content = await file.read()
    
    # 1. Analizar
    analysis = processor.analyze_file(content)
    if not analysis['valid']:
        raise HTTPException(400, detail=analysis['error'])

    # 2. Convertir a CSV para Celery
    csv_content = processor.dataframe_to_csv(analysis['dataframe'])
    operation = analysis['operation']

    # 3. Lanzar Tarea
    task = process_moodle_batch.delay(csv_content, operation)

    return {
        "message": "Archivo recibido y procesando.",
        "task_id": task.id,
        "operation_detected": operation,
        "rows_to_process": analysis['total_rows']
    }