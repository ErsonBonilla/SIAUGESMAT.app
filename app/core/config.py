import os
from pydantic_settings import BaseSettings
from typing import List, Optional

class Settings(BaseSettings):
    # --- INFORMACIÓN DEL PROYECTO ---
    PROJECT_NAME: str = "SIAUGESMAT - Universidad del Tolima"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    # --- SEGURIDAD ---
    # Clave para encriptar sesiones de NiceGUI / FastAPI
    SECRET_KEY: str = os.getenv("SECRET_KEY", "una_clave_muy_secreta_y_larga_123")
    # Token para proteger la API REST interna
    API_INTERNAL_TOKEN: str = os.getenv("API_INTERNAL_TOKEN", "ut-secret-2026")

    # --- BASE DE DATOS ---
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql://user:pass@localhost:5432/siaugesmat"
    )

    # --- REDIS / CELERY ---
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    CELERY_BROKER_URL: str = f"redis://{REDIS_HOST}:6379/0"
    CELERY_RESULT_BACKEND: str = f"redis://{REDIS_HOST}:6379/0"

    # --- MOODLE API ---
    MOODLE_API_URL: str = os.getenv("MOODLE_API_URL", "")
    MOODLE_API_TOKEN: str = os.getenv("MOODLE_API_TOKEN", "")

    # --- CONFIGURACIÓN DE CARGA DE ARCHIVOS ---
    MAX_UPLOAD_SIZE: int = 50 * 1024 * 1024  # 50 Megabytes
    ALLOWED_EXTENSIONS: List[str] = [".xlsx", ".xls"]

    class Config:
        # Esto permite que Pydantic lea directamente de un archivo .env si existe
        case_sensitive = True
        env_file = ".env"

# Instancia global para importar en todo el proyecto
settings = Settings()