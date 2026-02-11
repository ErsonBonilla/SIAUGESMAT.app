from fastapi import Header, HTTPException, status, Depends
from sqlalchemy.orm import Session
from app.core.deps import get_db
import os

# --- CONFIGURACIÓN DE SEGURIDAD ---
# En producción, podrías querer proteger los endpoints de la API con una API Key
API_KEY_NAME = "X-SIAUGESMAT-KEY"
API_KEY_SECRET = os.getenv("API_INTERNAL_TOKEN", "ut-secret-2026")

def validate_api_key(x_siaugesmat_key: str = Header(None)):
    """
    Dependencia para validar que las peticiones REST vengan de una fuente autorizada.
    Se usa en routes.py para proteger el endpoint de /upload.
    """
    if x_siaugesmat_key != API_KEY_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Credenciales de API inválidas o faltantes."
        )
    return x_siaugesmat_key

def get_current_active_user(db: Session = Depends(get_db)):
    """
    Espacio reservado para lógica de usuario actual (OAuth2/JWT).
    Si en el futuro integras el Login de la UT, aquí validarías el token.
    """
    # Por ahora es un placeholder para mantener la estructura profesional
    pass