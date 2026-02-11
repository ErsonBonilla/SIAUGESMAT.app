from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import os

# Obtener URL desde variables de entorno
# Ejemplo: postgresql://user:pass@db:5432/siaugesmat
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("La variable DATABASE_URL no está configurada en el .env")

# Crear el motor de base de datos (Engine)
# pool_pre_ping=True ayuda a reconectar si Postgres cierra la conexión inesperadamente
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

# Crear la fábrica de sesiones
# Cada vez que llamemos a SessionLocal(), obtenemos una nueva "conexión"
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)