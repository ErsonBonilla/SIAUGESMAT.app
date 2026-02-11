from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()

class FileUpload(Base):
    """
    Registra cada archivo Excel cargado al sistema.
    """
    __tablename__ = "file_uploads"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    upload_date = Column(DateTime, default=datetime.datetime.utcnow)
    operation_type = Column(String(50))  # CREATE_COURSE, ENROLL_USER, etc.
    status = Column(String(20), default="PROCESSING") # PROCESSING, COMPLETED, FAILED
    total_records = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    
    # Relación con los logs detallados
    logs = relationship("ProcessingLog", back_populates="upload", cascade="all, delete-orphan")

class ProcessingLog(Base):
    """
    Registra el resultado detallado de cada fila procesada contra Moodle.
    """
    __tablename__ = "processing_logs"

    id = Column(Integer, primary_key=True, index=True)
    upload_id = Column(Integer, ForeignKey("file_uploads.id"))
    
    identifier = Column(String(255)) # El email, username o shortname procesado
    action = Column(String(50))      # "CREATE", "ENROLL", "DELETE"
    status = Column(String(20))      # "SUCCESS", "ERROR"
    message = Column(Text)           # Mensaje de error de Moodle o confirmación
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

    upload = relationship("FileUpload", back_populates="logs")