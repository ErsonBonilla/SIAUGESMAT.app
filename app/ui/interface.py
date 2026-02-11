from nicegui import ui, run, app
from celery.result import AsyncResult
import asyncio

# Importaciones del proyecto
from app.services.data_processor import processor
from app.services.tasks import process_moodle_batch
from app.db.session import SessionLocal

# =======================================================
# GESTIÓN DE ESTADO DE SESIÓN
# =======================================================
class SessionState:
    def __init__(self):
        self.csv_data = None
        self.operation_type = None
        self.summary_text = ""

# =======================================================
# COMPONENTES DE LA INTERFAZ
# =======================================================

def init_ui():
    @ui.page('/')
    async def main_page():
        state = SessionState()
        
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
                ui.label('Arrastre el archivo generado por el sistema académico.').classes('text-gray-500 mb-4')
                
                upload_area = ui.upload(
                    label='Soportado: .xlsx, .xls, .csv',
                    auto_upload=True,
                    on_upload=lambda e: handle_upload(e, state),
                    max_files=1
                ).classes('w-full').props('accept=".xlsx, .xls, .csv"')

            # BLOQUE 2: AUDITORÍA
            audit_container = ui.column().classes('w-full gap-4 hidden')
            
            with audit_container:
                with ui.card().classes('w-full border-l-8 border-blue-500 bg-blue-50'):
                    with ui.row().classes('items-center'):
                        ui.icon('info', color='blue', size='md')
                        ui.label('Resultado del Análisis Inteligente').classes('text-lg font-bold')
                    
                    state_label = ui.label().classes('text-md mt-2 italic') 
                
                with ui.card().classes('w-full p-0 overflow-hidden'):
                    ui.label('Vista previa (Primeros 5 registros):').classes('p-4 font-bold text-gray-600')
                    preview_table = ui.table(columns=[], rows=[]).classes('w-full')

                with ui.row().classes('w-full justify-end gap-4 mt-4'):
                    ui.button('Cancelar', on_click=lambda: reset_ui(state, audit_container, upload_area)).props('outline color=red')
                    ui.button('CONFIRMAR Y PROCESAR', 
                              on_click=lambda: start_processing(state, audit_container, upload_area)).classes('bg-green-700 text-white font-bold px-6')

        # --- LÓGICA DE EVENTOS ---

        async def handle_upload(e, state):
            ui.notify('Analizando archivo...', type='info', position='top')
            try:
                content = await run.io_bound(e.content.read)
                result = await run.cpu_bound(processor.analyze_file, content)
                
                if not result['valid']:
                    ui.notify(f"Error: {result['error']}", type='negative', position='top', close_button=True, timeout=0)
                    upload_area.reset()
                    return

                state.csv_data = processor.dataframe_to_csv(result['dataframe'])
                state.operation_type = result['operation']
                state.summary_text = f"Operación: {result['operation']} - {result['summary']}"
                
                state_label.text = state.summary_text
                
                if result['preview']:
                    cols = [{'name': k, 'label': k.upper(), 'field': k, 'align': 'left'} for k in result['preview'][0].keys()]
                    preview_table.columns = cols
                    preview_table.rows = result['preview']

                audit_container.set_visibility(True)
                ui.notify('Archivo válido. Revise la vista previa.', type='positive')

            except Exception as ex:
                ui.notify(f'Error crítico al leer archivo: {str(ex)}', type='negative')
                upload_area.reset()

        def reset_ui(state, container, upload):
            state.csv_data = None
            state.operation_type = None
            container.set_visibility(False)
            upload.reset()

        def start_processing(state, container, upload):
            if not state.csv_data:
                ui.notify('No hay datos para procesar.', type='warning')
                return

            try:
                # 1. Enviar tarea a Celery
                task = process_moodle_batch.delay(state.csv_data, state.operation_type)
                
                # 2. Crear un diálogo dinámico de seguimiento
                dialog = ui.dialog().classes('w-96')
                with dialog, ui.card().classes('w-full items-center p-6 gap-4'):
                    title = ui.label('Procesando en Moodle...').classes('text-xl font-bold text-primary')
                    status_text = ui.label('Iniciando worker...').classes('text-gray-600')
                    
                    spinner = ui.spinner('dots', size='xl', color='primary')
                    
                    # Contenedor de resultados (Oculto al inicio)
                    result_view = ui.column().classes('w-full items-center hidden gap-2')
                    with result_view:
                        ui.icon('check_circle', color='green', size='48px')
                        res_total = ui.label().classes('text-lg font-bold')
                        res_success = ui.label().classes('text-md text-green-700 font-bold')
                        res_errors = ui.label().classes('text-md text-red-700 font-bold')

                    close_btn = ui.button('Cerrar', on_click=dialog.close).classes('w-full mt-4 hidden')

                dialog.open()
                reset_ui(state, container, upload)

                # 3. Lógica de Polling (Consultar Celery cada 2 segundos)
                def check_task_status():
                    # Consultamos el backend de Redis a través de Celery
                    res = AsyncResult(task.id)
                    
                    if res.ready():
                        # La tarea terminó (Éxito o Fallo)
                        timer.cancel() # Detenemos el reloj
                        spinner.set_visibility(False)
                        close_btn.set_visibility(True)
                        
                        if res.successful():
                            data = res.result
                            title.text = '¡Carga Completada!'
                            title.classes(replace='text-green-700')
                            status_text.set_visibility(False)
                            
                            # Mostrar métricas reales
                            res_total.text = f"Total procesados: {data.get('total', 0)}"
                            res_success.text = f"✅ Éxitos: {data.get('success', 0)}"
                            res_errors.text = f"❌ Errores: {data.get('errors', 0)}"
                            result_view.set_visibility(True)
                        else:
                            title.text = 'Error de Procesamiento'
                            title.classes(replace='text-red-700')
                            status_text.text = str(res.result)
                    else:
                        # Sigue en proceso
                        status_text.text = f"Estado actual: {res.status}..."

                # Iniciamos el reloj que llama a la función cada 2.0 segundos
                timer = ui.timer(2.0, check_task_status)
                
            except Exception as ex:
                ui.notify(f'Error de conexión con el Worker: {ex}', type='negative')

if __name__ in {"__main__", "__mp_main__"}:
    init_ui()
    ui.run(port=8080, title="SIAUGESMAT - UT", favicon="🎓", show=False)