import datetime
import enum
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

def get_utc_now():
    """
    Retorna la fecha y hora actual en UTC con información de zona horaria.
    Reemplaza a datetime.utcnow() que está deprecado en Python 3.12.
    """
    return datetime.datetime.now(datetime.timezone.utc)

# --- ENUMS PARA INTEGRIDAD DE DATOS ---
class OperationType(enum.Enum):
    CREATE_COURSE = "CREATE_COURSE"
    ENROLL_USER = "ENROLL_USER"
    CREATE_USER = "CREATE_USER"
    DELETE_COURSE = "DELETE_COURSE"
    DELETE_USER = "DELETE_USER"
    UPDATE_VISIBILITY = "UPDATE_VISIBILITY"

class StatusType(enum.Enum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRY = "RETRY"

# --- MODELOS ---
class FileUpload(Base):
    """
    Registra cada archivo Excel cargado al sistema.
    """
    __tablename__ = "file_uploads"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    upload_date = Column(DateTime(timezone=True), default=get_utc_now)
    
    # 💡 MEJORA: Uso de Enums a nivel de base de datos
    operation_type = Column(Enum(OperationType))  
    status = Column(Enum(StatusType), default=StatusType.PROCESSING) 
    
    total_records = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    
    # 💡 MEJORA: Se eliminó cascade="all, delete-orphan" para proteger el historial de auditoría
    logs = relationship("ProcessingLog", back_populates="upload")

class ProcessingLog(Base):
    """
    Registra el resultado detallado de cada fila procesada contra Moodle.
    """
    __tablename__ = "processing_logs"

    id = Column(Integer, primary_key=True, index=True)
    
    # 🚨 CRÍTICO: index=True agregado para evitar Sequential Scans en el visualizador
    upload_id = Column(Integer, ForeignKey("file_uploads.id"), index=True)
    
    # 💡 MEJORA: String sin límite de longitud para evitar errores de truncamiento en Celery
    identifier = Column(String) 
    action = Column(String(50))      
    
    # 🚨 CRÍTICO: index=True agregado para acelerar los gráficos de Plotly
    status = Column(String(20), index=True)      
    
    message = Column(Text)           
    
    # 💡 MEJORA: Campo JSONB nativo de PostgreSQL para almacenar la respuesta cruda de Moodle
    # Esto permite hacer consultas SQL estructuradas sobre los errores en el futuro
    details = Column(JSONB, nullable=True)

    timestamp = Column(DateTime(timezone=True), default=get_utc_now)

    upload = relationship("FileUpload", back_populates="logs")