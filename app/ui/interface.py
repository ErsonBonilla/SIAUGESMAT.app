from nicegui import ui, app
from app.services.data_processor import processor
from app.services.tasks import process_moodle_batch
from app.services.visualizer import visualizer
from app.core.deps import SessionLocal
import pandas as pd
import io

# =======================================================
# GESTIÓN DE ESTADO DE SESIÓN
# =======================================================
class SessionState:
    def __init__(self):
        self.csv_data = None
        self.operation_type = None
        self.preview_rows = []
        self.preview_cols = []
        self.summary_text = ""
        self.is_analyzed = False

# =======================================================
# COMPONENTES DE LA INTERFAZ
# =======================================================

def init_ui():
    @ui.page('/')
    def main_page():
        # Estado local por cliente
        state = SessionState()
        
        # Estilos generales (Colores UT: Vinotinto y Blanco)
        ui.colors(primary='#8b0000', secondary='#666666', accent='#111b1e')

        # --- HEADER ---
        with ui.header().classes('bg-primary text-white items-center p-4 shadow-2'):
            ui.icon('hub', size='32px')
            ui.label('SIAUGESMAT | Universidad del Tolima').classes('text-xl font-bold ml-2')
            ui.space()
            ui.button(icon='refresh', on_click=lambda: ui.open('/')).props('flat color=white')

        # --- CUERPO PRINCIPAL ---
        with ui.column().classes('w-full max-w-5xl mx-auto p-6 gap-6'):
            
            # BLOQUE 1: CARGA DE ARCHIVO
            with ui.card().classes('w-full p-6'):
                ui.label('1. Carga de Datos (Excel)').classes('text-lg font-bold mb-2')
                ui.label('Arrastre el archivo generado por el sistema académico para su análisis.').classes('text-gray-500 mb-4')
                
                upload_area = ui.upload(
                    label='Soportado: .xlsx, .xls',
                    auto_upload=True,
                    on_upload=lambda e: handle_upload(e, state),
                    max_files=1
                ).classes('w-full').props('accept=".xlsx, .xls"')

            # BLOQUE 2: AUDITORÍA (Se muestra tras analizar)
            audit_container = ui.column().classes('w-full gap-4 hidden')
            with audit_container:
                with ui.card().classes('w-full border-l-8 border-blue-500 bg-blue-50'):
                    with ui.row().classes('items-center'):
                        ui.icon('info', color='blue', size='md')
                        ui.label('Resultado del Análisis Inteligente').classes('text-lg font-bold')
                    
                    state_label = ui.label().bind_text_from(state, 'summary_text').classes('text-md mt-2 italic')
                
                # Tabla de Vista Previa
                with ui.card().classes('w-full p-0 overflow-hidden'):
                    ui.label('Vista previa de los primeros registros:').classes('p-4 font-bold text-gray-600')
                    preview_table = ui.table(columns=[], rows=[]).classes('w-full')

                # Botones de Acción
                with ui.row().classes('w-full justify-end gap-4 mt-4'):
                    ui.button('Cancelar', on_click=lambda: reset_ui(state, audit_container, upload_area)).props('outline color=red')
                    ui.button('CONFIRMAR Y EJECUTAR EN MOODLE', 
                              on_click=lambda: start_processing(state, audit_container, upload_area)).classes('bg-green-700 text-white font-bold px-6')

            # BLOQUE 3: HISTORIAL RÁPIDO / DASHBOARD (Opcional)
            with ui.expansion('Ver historial reciente de cargas', icon='history').classes('w-full bg-gray-100 rounded-lg'):
                ui.label('Aquí se mostrarán las últimas 5 operaciones y su tasa de éxito.')

        # --- LÓGICA DE EVENTOS ---

        async def handle_upload(e, state):
            """Analiza el archivo y prepara la auditoría."""
            content = e.content.read()
            result = processor.analyze_file(content)
            
            if not result['valid']:
                ui.notify(f"Error: {result['error']}", type='negative', position='top')
                upload_area.reset()
                return

            # Actualizar estado
            state.csv_data = processor.dataframe_to_csv(result['dataframe'])
            state.operation_type = result['operation']
            state.summary_text = f"Operación Detectada: {result['operation']} - {result['summary']}"
            
            # Configurar Tabla
            if result['preview']:
                cols = [{'name': k, 'label': k.upper(), 'field': k, 'align': 'left'} for k in result['preview'][0].keys()]
                preview_table.columns = cols
                preview_table.rows = result['preview']

            # Mostrar sección
            audit_container.set_visibility(True)
            ui.notify('Archivo analizado con éxito.', type='positive')

        def reset_ui(state, container, upload):
            state.__init__()
            container.set_visibility(False)
            upload.reset()

        async def start_processing(state, container, upload):
            """Dispara la tarea en Celery."""
            try:
                # Enviar a Celery (Worker)
                task = process_moodle_batch.delay(state.csv_data, state.operation_type)
                
                ui.notify(f'Tarea enviada al servidor (ID: {task.id})', type='ongoing', position='bottom-right')
                
                # Feedback de éxito y reinicio
                with ui.dialog() as dialog, ui.card():
                    ui.label('¡Proceso Iniciado!').classes('text-xl font-bold text-green-700')
                    ui.label(f'Se están procesando los registros para la operación {state.operation_type}. '
                             'Puede monitorear el progreso en la pestaña de logs.')
                    ui.button('Aceptar', on_click=dialog.close)
                dialog.open()
                
                reset_ui(state, container, upload)
                
            except Exception as ex:
                ui.notify(f'Error al conectar con el worker: {ex}', type='negative')

# Para ejecución directa de prueba
if __name__ in {"__main__", "__mp_main__"}:
    init_ui()
    ui.run(port=8080, title="SIAUGESMAT - UT")