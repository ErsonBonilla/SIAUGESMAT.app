import os
from fastapi import FastAPI
from nicegui import ui, app as nicegui_app

# Importaciones locales
from app.core.config import settings
from app.api.routes import router as api_router
from app.ui.interface import init_ui
from app.db.init_db import init_database

# 1. INICIALIZACIÓN DE LA BASE DE DATOS
# Crea las tablas en PostgreSQL si no existen al arrancar
try:
    init_database()
    print("✅ Base de datos sincronizada correctamente.")
except Exception as e:
    print(f"❌ Error al conectar con la base de datos: {e}")

# 2. CREACIÓN DE LA APP FASTAPI
# NiceGUI corre "encima" de FastAPI, permitiéndonos tener ambos mundos
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Sistema Intermediario de Automatización para Moodle - UT"
)

# 3. REGISTRO DE RUTAS API
# Esto habilita los endpoints /health, /task y /upload
app.include_router(api_router, prefix=settings.API_V1_STR, tags=["API Moodle"])

# 4. CONFIGURACIÓN DE SEGURIDAD Y ARCHIVOS ESTÁTICOS
# (Opcional) Si tienes imágenes o logos en la carpeta 'static'
# app.mount("/static", StaticFiles(directory="static"), name="static")

# 5. INICIALIZACIÓN DE LA INTERFAZ NICEGUI
# Pasamos la instancia de FastAPI a NiceGUI para que compartan el mismo servidor
init_ui()

# 6. PUNTO DE ENTRADA PARA DESARROLLO (Uvicorn)
# En producción (Docker/K8s) se usa el comando definido en el Dockerfile
if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=settings.PROJECT_NAME,
        host="0.0.0.0",
        port=8080,
        storage_secret=settings.SECRET_KEY, # Importante para sesiones de usuario
        dark=False,  # Puedes cambiarlo a True según el gusto de la UT
        reload=False # En producción/Docker siempre debe ser False
    )