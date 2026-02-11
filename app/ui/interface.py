from nicegui import ui, run, app
from app.services.data_processor import processor
# Asegúrate de que tasks.py exista (corrección anterior)
from app.services.tasks import process_moodle_batch
from app.db.session import SessionLocal  # <--- CORRECCIÓN DE IMPORTACIÓN
import asyncio

# =======================================================
# GESTIÓN DE ESTADO DE SESIÓN
# =======================================================
class SessionState:
    def __init__(self):
        self.csv_data = None
        self.operation_type = None
        self.summary_text = ""
        # No guardamos el dataframe completo en memoria de sesión para ahorrar RAM

# =======================================================
# COMPONENTES DE LA INTERFAZ
# =======================================================

def init_ui():
    @ui.page('/')
    async def main_page():
        # Estado local por cliente (pestaña del navegador)
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
                ui.label('Arrastre el archivo generado por el sistema académico.').classes('text-gray-500 mb-4')
                
                upload_area = ui.upload(
                    label='Soportado: .xlsx, .xls, .csv',
                    auto_upload=True,
                    on_upload=lambda e: handle_upload(e, state),
                    max_files=1
                ).classes('w-full').props('accept=".xlsx, .xls, .csv"')

            # BLOQUE 2: AUDITORÍA (Oculto inicialmente)
            audit_container = ui.column().classes('w-full gap-4 hidden')
            
            with audit_container:
                # Tarjeta de Resumen
                with ui.card().classes('w-full border-l-8 border-blue-500 bg-blue-50'):
                    with ui.row().classes('items-center'):
                        ui.spinner('dots', size='lg', color='blue').bind_visibility_from(state, 'processing') # Placeholder visual
                        ui.icon('info', color='blue', size='md')
                        ui.label('Resultado del Análisis Inteligente').classes('text-lg font-bold')
                    
                    # Etiqueta vinculada al texto del estado
                    state_label = ui.label().classes('text-md mt-2 italic') 
                
                # Tabla de Vista Previa
                with ui.card().classes('w-full p-0 overflow-hidden'):
                    ui.label('Vista previa (Primeros 5 registros):').classes('p-4 font-bold text-gray-600')
                    preview_table = ui.table(columns=[], rows=[]).classes('w-full')

                # Botones de Acción
                with ui.row().classes('w-full justify-end gap-4 mt-4'):
                    ui.button('Cancelar', on_click=lambda: reset_ui(state, audit_container, upload_area)).props('outline color=red')
                    ui.button('CONFIRMAR Y PROCESAR', 
                              on_click=lambda: start_processing(state, audit_container, upload_area)).classes('bg-green-700 text-white font-bold px-6')

        # --- LÓGICA DE EVENTOS (ASYNC / NON-BLOCKING) ---

        async def handle_upload(e, state):
            """
            Maneja la carga del archivo.
            MEJORA: Usa run.io_bound y run.cpu_bound para no congelar la UI.
            """
            ui.notify('Analizando archivo...', type='info', position='top')
            
            try:
                # 1. Leer el archivo (I/O Bound - Fuera del Main Loop)
                content = await run.io_bound(e.content.read)
                
                # 2. Procesar Pandas (CPU Bound - Fuera del Main Loop)
                # Esto es vital para que si el Excel es grande, el servidor no se "cuelgue"
                result = await run.cpu_bound(processor.analyze_file, content)
                
                if not result['valid']:
                    ui.notify(f"Error: {result['error']}", type='negative', position='top', close_button=True, timeout=0)
                    upload_area.reset()
                    return

                # 3. Actualizar Estado (Main Loop)
                # Convertimos el DF a CSV string aquí para pasarlo luego a Celery
                state.csv_data = processor.dataframe_to_csv(result['dataframe'])
                state.operation_type = result['operation']
                state.summary_text = f"Operación: {result['operation']} - {result['summary']}"
                
                # Actualizar UI
                state_label.text = state.summary_text
                
                if result['preview']:
                    # Generar columnas dinámicamente para la tabla
                    cols = [{'name': k, 'label': k.upper(), 'field': k, 'align': 'left'} for k in result['preview'][0].keys()]
                    preview_table.columns = cols
                    preview_table.rows = result['preview']

                audit_container.set_visibility(True)
                ui.notify('Archivo válido. Revise la vista previa.', type='positive')

            except Exception as ex:
                ui.notify(f'Error crítico al leer archivo: {str(ex)}', type='negative')
                upload_area.reset()

        def reset_ui(state, container, upload):
            """Limpia el estado y oculta la sección de auditoría."""
            state.csv_data = None
            state.operation_type = None
            container.set_visibility(False)
            upload.reset()

        async def start_processing(state, container, upload):
            """Envía la tarea a Celery (Redis)."""
            if not state.csv_data:
                ui.notify('No hay datos para procesar.', type='warning')
                return

            try:
                # Enviar tarea asíncrona a Celery
                # .delay() es el método estándar de Celery para invocar tareas
                task = process_moodle_batch.delay(state.csv_data, state.operation_type)
                
                # Mostrar diálogo de éxito
                with ui.dialog() as dialog, ui.card():
                    ui.label('¡Proceso Iniciado!').classes('text-xl font-bold text-green-700')
                    ui.label(f'ID de Tarea: {task.id}')
                    ui.label('El sistema está procesando los registros en segundo plano.')
                    ui.label('Puede cerrar esta ventana o cargar otro archivo.')
                    ui.button('Entendido', on_click=dialog.close)
                
                dialog.open()
                reset_ui(state, container, upload)
                
            except Exception as ex:
                ui.notify(f'Error de conexión con el Worker: {ex}', type='negative')

# Punto de entrada para desarrollo local
if __name__ in {"__main__", "__mp_main__"}:
    # init_database() # Descomentar si se corre localmente sin docker-compose previo
    init_ui()
    ui.run(
        port=8080, 
        title="SIAUGESMAT - UT",
        favicon="🎓",
        show=False # No abrir navegador automáticamente en servidores
    )