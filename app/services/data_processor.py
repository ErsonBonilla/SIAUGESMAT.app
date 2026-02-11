import pandas as pd
import io
import logging
from typing import Dict, Any, List

# Configuración de Logging
logger = logging.getLogger(__name__)

class MoodleDataProcessor:
    def __init__(self):
        # Definición de columnas esperadas para identificación de operaciones
        self.cols_create_course = [
            'nombre cat', 'cod. programa', 'cod. curso', 
            'semestre', 'grupo', 'nombre curso'
        ]
        self.cols_create_user = ['username', 'firstname', 'lastname', 'email']

    def _clean_text(self, text: Any) -> str:
        """Limpia valores nulos y espacios en blanco."""
        if pd.isna(text):
            return ""
        return str(text).strip()

    def _get_cat_prefix(self, nombre_cat: str) -> str:
        """
        Regla de Negocio:
        - Si es 'APARTADO' -> 'URA'
        - Sino -> Tres primeras letras en mayúscula.
        """
        nombre_cat = self._clean_text(nombre_cat).upper()
        if "APARTADO" in nombre_cat:
            return "URA"
        return nombre_cat[:3]

    def _format_two_digits(self, val: Any) -> str:
        """Asegura que el valor tenga al menos 2 dígitos (ej: 5 -> 05)."""
        clean_val = self._clean_text(val)
        return clean_val.zfill(2)

    def analyze_file(self, file_content: bytes) -> Dict[str, Any]:
        """
        Método Principal:
        1. Lee el Excel.
        2. Analiza las columnas para determinar la operación (Crear, Borrar, Matricular).
        3. Si es creación de cursos, aplica las fórmulas de nombres (shortname, fullname).
        4. Retorna un resumen y el CSV listo para procesar.
        """
        try:
            # Cargar archivo
            df = pd.read_excel(io.BytesIO(file_content))
            
            # Normalizar columnas a minúsculas para facilitar detección
            df.columns = [str(c).strip().lower() for c in df.columns]
            columns = list(df.columns)
            
            operation_type = "UNKNOWN"
            summary = "No se pudo determinar la operación."
            
            # --- LÓGICA DE DETECCIÓN INTELIGENTE ---

            # CASO 1: ELIMINAR CURSOS
            if 'shortname' in columns and 'delete' in columns:
                operation_type = "DELETE_COURSE"
                # Filtrar solo los que tienen delete=1
                df = df[df['delete'] == 1]
                summary = f"Se eliminarán {len(df)} cursos."

            # CASO 2: ELIMINAR USUARIOS
            elif 'username' in columns and 'delete' in columns:
                operation_type = "DELETE_USER"
                df = df[df['delete'] == 1]
                summary = f"Se eliminarán {len(df)} usuarios."

            # CASO 3: CAMBIAR VISIBILIDAD
            elif 'shortname' in columns and 'visible' in columns:
                operation_type = "UPDATE_VISIBILITY"
                # Contamos cuántos se van a ocultar (0) y mostrar (1)
                hidden_count = df[df['visible'] == 0].shape[0]
                summary = f"Se actualizará visibilidad: {hidden_count} cursos se ocultarán."

            # CASO 4: CREAR/MATRICULAR USUARIOS
            # Prioridad: Si tiene firstname/email es CREAR, si solo tiene username+curso es MATRICULAR
            elif all(x in columns for x in self.cols_create_user):
                operation_type = "CREATE_USER"
                summary = f"Se crearán {len(df)} nuevos usuarios en la plataforma."
                # Validar password por defecto si no existe
                if 'password' not in columns:
                    df['password'] = 'Cambiar123!' 

            # CASO 5: MATRICULAR USUARIOS (Enroll)
            elif 'username' in columns and ('shortname' in columns or 'course1' in columns):
                operation_type = "ENROLL_USER"
                # Normalizar nombre de columna de curso
                if 'course1' in columns and 'shortname' not in columns:
                    df['shortname'] = df['course1']
                
                # Asignar rol por defecto si no viene
                if 'role1' not in columns:
                    df['role1'] = 'student'
                
                summary = f"Se procesarán {len(df)} matriculaciones."

            # CASO 6: CREAR CURSOS (Lógica Compleja)
            elif all(col in columns for col in self.cols_create_course):
                operation_type = "CREATE_COURSE"
                df = self._transform_create_course_data(df)
                summary = f"Se crearán {len(df)} cursos académicos con nomenclatura UT."

            else:
                missing = [c for c in self.cols_create_course if c not in columns]
                return {
                    "valid": False,
                    "error": f"Formato no reconocido. Faltan columnas clave (ej: {missing})"
                }

            # Si el DF quedó vacío tras filtrar (ej. delete=0 en todos)
            if df.empty and operation_type in ["DELETE_COURSE", "DELETE_USER"]:
                return {
                    "valid": False, 
                    "error": "El archivo es válido, pero no hay registros marcados con 'delete=1'."
                }

            return {
                "valid": True,
                "operation": operation_type,
                "summary": summary,
                "total_rows": len(df),
                "preview": df.head(5).fillna('').to_dict('records'),
                "dataframe": df  # Objeto DataFrame listo para ser convertido a CSV
            }

        except Exception as e:
            logger.error(f"Error analizando archivo: {str(e)}")
            return {"valid": False, "error": f"Error procesando el archivo: {str(e)}"}

    def _transform_create_course_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica las reglas de transformación para generar nombres de cursos.
        Basado en los campos: NOMBRE CAT, COD. PROGRAMA, COD. CURSO, SEMESTRE, GRUPO
        """
        # 1. Limpieza y preparación de columnas base
        df['clean_cat'] = df['nombre cat'].apply(self._get_cat_prefix)
        df['clean_prog'] = df['cod. programa'].apply(self._format_two_digits)
        df['clean_curso'] = df['cod. curso'].apply(self._clean_text)
        df['clean_semestre'] = df['semestre'].apply(self._clean_text)
        df['clean_grupo'] = df['grupo'].apply(self._clean_text)

        # 2. Generación de SHORTNAME
        # Formato: CAT_PROG_CURSO_sSEMESTRE_G-GRUPO (Ej: IBA_04_0202_s09_G-01)
        df['shortname'] = (
            df['clean_cat'] + "_" + 
            df['clean_prog'] + "_" + 
            df['clean_curso'] + "_s" + 
            df['clean_semestre'] + "_G-" + 
            df['clean_grupo']
        )

        # 3. Generación de FULLNAME
        # Formato: NOMBRE CURSO + Grupo + GRUPO
        df['fullname'] = df['nombre curso'] + " Grupo " + df['clean_grupo']

        # 4. Generación de IDNUMBER (Categoría) y CATEGORY
        # Se asume que la categoría ya existe con este IDNUMBER
        df['category_idnumber'] = (
            df['clean_cat'] + "_" + 
            df['clean_prog'] + "_s" + 
            df['clean_semestre']
        )

        # 5. Generación de TEMPLATECOURSE (Para duplicar si es necesario)
        # Formato: PORTAFOLIO_PROG_CURSO_sSEMESTRE
        df['templatecourse'] = (
            "PORTAFOLIO_" + 
            df['clean_prog'] + "_" + 
            df['clean_curso'] + "s" + 
            df['clean_semestre']
        )
        
        # 6. Campos obligatorios de Moodle
        df['visible'] = 1
        df['format'] = 'onetopic' # Formato pestaña (común en universidades)

        # Selección final de columnas útiles para la API
        cols_export = ['shortname', 'fullname', 'category_idnumber', 'templatecourse', 'visible', 'format']
        return df[cols_export]

    def dataframe_to_csv(self, df: pd.DataFrame) -> str:
        """Convierte el DataFrame procesado a string CSV UTF-8."""
        return df.to_csv(index=False, encoding='utf-8', sep=',')

# Instancia global para importar en otros módulos
processor = MoodleDataProcessor()