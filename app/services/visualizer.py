import pandas as pd
import plotly.graph_objects as go
from typing import Dict, List, Any
from sqlalchemy.orm import Session
from app.models.models import ProcessingLog, FileUpload

class DataVisualizer:
    """
    Transforma datos crudos de la DB en componentes visuales (Plotly)
    para la interfaz de NiceGUI.
    """

    @staticmethod
    def get_summary_stats(db: Session, upload_id: int) -> Dict[str, Any]:
        """Obtiene métricas rápidas de una carga específica."""
        upload = db.query(FileUpload).filter(FileUpload.id == upload_id).first()
        if not upload:
            return {"total": 0, "success": 0, "errors": 0}
        
        return {
            "total": upload.total_records,
            "success": upload.success_count,
            "errors": upload.error_count,
            "status": upload.status
        }

    @staticmethod
    def create_success_pie_chart(success: int, errors: int):
        """
        Genera un gráfico de torta mostrando la efectividad del proceso.
        """
        fig = go.Figure(data=[go.Pie(
            labels=['Éxito', 'Errores'],
            values=[success, errors],
            hole=.4,
            marker_colors=['#2ecc71', '#e74c3c'] # Verde y Rojo
        )])
        
        fig.update_layout(
            margin=dict(t=0, b=0, l=0, r=0),
            showlegend=True,
            height=250
        )
        return fig

    @staticmethod
    def create_error_distribution_chart(db: Session, upload_id: int):
        """
        Analiza los mensajes de error para ver cuál es el problema más común
        (Ej: "Usuario ya existe" vs "Curso no encontrado").
        """
        # Obtenemos solo los logs con error
        logs = db.query(ProcessingLog).filter(
            ProcessingLog.upload_id == upload_id,
            ProcessingLog.status == "ERROR"
        ).all()

        if not logs:
            return None

        df = pd.DataFrame([{"msg": log.message} for log in logs])
        error_counts = df['msg'].value_counts().reset_index()
        error_counts.columns = ['error', 'count']

        fig = go.Figure(go.Bar(
            x=error_counts['count'],
            y=error_counts['error'],
            orientation='h',
            marker_color='#34495e'
        ))

        fig.update_layout(
            title="Distribución de Errores",
            margin=dict(t=30, b=0, l=0, r=0),
            height=300,
            xaxis_title="Cantidad",
            yaxis=dict(autorange="reversed") # El error más frecuente arriba
        )
        return fig

# Instancia para exportar
visualizer = DataVisualizer()