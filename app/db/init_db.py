from app.db.session import engine
from app.models.models import Base

def init_database():
    """Crea todas las tablas definidas en los modelos."""
    # En un entorno profesional usarías Alembic para migraciones,
    # pero para iniciar el proyecto, esto creará las tablas si no existen.
    Base.metadata.create_all(bind=engine)