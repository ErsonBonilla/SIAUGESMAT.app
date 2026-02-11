import pandas as pd
import io
from typing import Dict, Any, List
import logging

# Configuración de logging
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Clase encargada de ingerir, limpiar y validar los archivos Excel/CSV
    antes de enviarlos al servicio de sincronización con Moodle.
    """

    REQUIRED_COLUMNS = {
        "CREATE_USER": {"username", "firstname", "lastname", "email", "password"},
        "ENROLL_USER": {"username", "shortname", "role"}, # 'shortname' se refiere al curso
        "CREATE_COURSE": {"fullname", "shortname", "category_id"}
    }

    def _clean_text(self, text: Any) -> str:
        """
        Limpia espacios en blanco y maneja valores nulos de forma segura.
        Corrección: Evita convertir None/NaN en la cadena literal "None" o "nan".
        """
        if pd.isna(text) or text is None:
            return ""
        return str(text).strip()

    def analyze_file(self, file_content: bytes) -> Dict[str, Any]:
        """
        Analiza el archivo binario cargado, determina qué operación se va a realizar
        y devuelve una vista previa o errores.
        """
        result = {
            "valid": False,
            "operation": None,
            "dataframe": None, # Se usará internamente, no serializable a JSON directo
            "preview": [],
            "error": None,
            "summary": ""
        }

        try:
            # 1. Intentar leer el archivo (Soporte Excel y CSV)
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception:
                # Si falla Excel, intentamos CSV con encoding utf-8 y luego latin-1
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='latin-1')

            # 2. Normalizar encabezados (Trim y Lowercase)
            # Esto permite que el usuario suba "User Name" y lo detectemos como "username"
            df.columns = [str(col).strip().lower().replace(" ", "") for col in df.columns]

            # 3. Limpieza de datos (Trim de celdas y manejo de nulos)
            # NOTA: En Pandas 2.0+, applymap fue renombrado a map para DataFrames
            df = df.map(self._clean_text)

            # 4. Eliminar filas totalmente vacías
            df.replace("", float("nan"), inplace=True)
            df.dropna(how='all', inplace=True)
            df.fillna("", inplace=True) # Volver a poner strings vacíos para el procesamiento

            if df.empty:
                result["error"] = "El archivo está vacío o no contiene datos legibles."
                return result

            # 5. Detectar Operación basada en columnas
            operation, missing = self._detect_operation(df.columns)
            
            if not operation:
                result["error"] = (
                    f"No se pudo detectar la operación automáticamente. "
                    f"Columnas encontradas: {list(df.columns)}. "
                    f"Asegúrese de usar las cabeceras estándar (ej: username, email, shortname)."
                )
                return result

            if missing:
                result["error"] = f"Para la operación {operation}, faltan las columnas: {missing}"
                return result

            # 6. Preparar resultado exitoso
            result["valid"] = True
            result["operation"] = operation
            result["dataframe"] = df
            result["summary"] = f"Se detectaron {len(df)} registros."
            # Convertimos a dict solo las primeras 5 filas para la UI
            result["preview"] = df.head(5).to_dict(orient='records')
            
            return result

        except Exception as e:
            logger.error(f"Error procesando archivo: {e}")
            result["error"] = f"Error interno al leer el archivo: {str(e)}"
            return result

    def _detect_operation(self, columns: List[str]):
        """
        Infiere la intención del usuario basándose en las columnas presentes.
        Retorna: (NombreOperacion, ColumnasFaltantes)
        """
        columns_set = set(columns)

        # Lógica de prioridad:
        # 1. Si tiene 'password' y 'email', probablemente es CREAR USUARIOS
        if {"username", "email"}.issubset(columns_set):
            required = self.REQUIRED_COLUMNS["CREATE_USER"]
            missing = required - columns_set
            # Permitimos que falten algunas opcionales si quisiéramos, 
            # pero por seguridad pedimos todas las críticas.
            if len(missing) == 0:
                return "CREATE_USER", None
        
        # 2. Si tiene 'fullname' y 'category_id', es CREAR CURSOS
        if {"fullname", "category_id"}.issubset(columns_set):
            required = self.REQUIRED_COLUMNS["CREATE_COURSE"]
            missing = required - columns_set
            return "CREATE_COURSE", missing

        # 3. Si tiene 'shortname' y 'username', es MATRICULACIÓN
        # (shortname se refiere al curso)
        if {"username", "shortname"}.issubset(columns_set):
            required = self.REQUIRED_COLUMNS["ENROLL_USER"]
            # 'role' suele ser opcional (default student), lo quitamos de obligatorios si falta
            missing = required - columns_set
            if "role" in missing:
                missing.remove("role") 
            return "ENROLL_USER", list(missing) if missing else None

        return None, None

    def dataframe_to_csv(self, df: pd.DataFrame) -> str:
        """Convierte el DF a string CSV para pasarlo a Celery/Redis."""
        return df.to_csv(index=False)

# Instancia global
processor = DataProcessor()