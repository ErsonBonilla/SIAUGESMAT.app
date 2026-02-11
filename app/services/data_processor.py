import pandas as pd
import io
import re
from typing import Dict, Any, List, Tuple, Optional
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
        "ENROLL_USER": {"username", "shortname", "role"}, 
        "CREATE_COURSE": {"fullname", "shortname", "category_id"}
    }

    def _clean_general_text(self, text: Any) -> str:
        """
        Limpieza estándar para campos de texto (Nombres, Descripciones).
        Mantiene espacios y acentos, pero elimina espacios al inicio/final
        y maneja valores nulos.
        """
        if pd.isna(text) or text is None:
            return ""
        # Convertir a string y quitar espacios extra a los lados
        return str(text).strip()

    def _clean_username(self, text: Any) -> str:
        """
        Sanitización ESTRICTA para el campo 'username'.
        Moodle prohíbe espacios y mayúsculas en el username.
        
        Transformaciones:
        1. Convierte a minúsculas.
        2. Elimina cualquier caracter que NO sea: a-z, 0-9, ., -, _, @
        3. Esto elimina implícitamente espacios, tildes y ñ.
        """
        if pd.isna(text) or text is None:
            return ""
        
        # 1. Convertir a string y minúsculas
        s = str(text).lower().strip()
        
        # 2. Eliminar caracteres no permitidos por Moodle (incluyendo espacios)
        # Regex: Mantener solo alfanuméricos, puntos, guiones, guiones bajos y arrobas.
        s = re.sub(r'[^a-z0-9\.\-\@_]', '', s)
        
        return s

    def analyze_file(self, file_content: bytes) -> Dict[str, Any]:
        """
        Analiza el archivo binario cargado, normaliza los datos y determina la operación.
        """
        result = {
            "valid": False,
            "operation": None,
            "dataframe": None,
            "preview": [],
            "error": None,
            "summary": ""
        }

        try:
            # 1. Intentar leer el archivo (Soporte Excel y CSV)
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception:
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='latin-1')

            # 2. Normalizar encabezados
            # Elimina espacios y convierte a minúsculas (ej: "E-mail " -> "email")
            df.columns = [str(col).strip().lower().replace(" ", "").replace("-", "") for col in df.columns]

            # 3. Limpieza General de Datos
            # Aplicamos limpieza básica a todo el DataFrame
            df = df.map(self._clean_general_text)

            # 4. Sanitización Específica de USERNAME (CORRECCIÓN CRÍTICA)
            if 'username' in df.columns:
                df['username'] = df['username'].map(self._clean_username)
                
                # Validación extra: Verificar si quedaron usernames vacíos tras la limpieza
                if (df['username'] == "").any():
                    result["error"] = "Algunos usuarios quedaron vacíos tras la limpieza (posibles caracteres inválidos)."
                    return result

            # 5. Eliminar filas totalmente vacías
            df.replace("", float("nan"), inplace=True)
            df.dropna(how='all', inplace=True)
            df.fillna("", inplace=True)

            if df.empty:
                result["error"] = "El archivo está vacío o no contiene datos legibles."
                return result

            # 6. Detectar Operación
            operation, missing = self._detect_operation(df.columns)
            
            if not operation:
                result["error"] = (
                    f"No se pudo detectar la operación. Columnas encontradas: {list(df.columns)}. "
                    f"Revise que los encabezados sean correctos (ej: username, shortname)."
                )
                return result

            if missing:
                result["error"] = f"Para la operación {operation}, faltan las columnas: {missing}"
                return result

            # 7. Preparar resultado exitoso
            result["valid"] = True
            result["operation"] = operation
            result["dataframe"] = df
            result["summary"] = f"Se detectaron {len(df)} registros listos para procesar."
            result["preview"] = df.head(5).to_dict(orient='records')
            
            return result

        except Exception as e:
            logger.error(f"Error procesando archivo: {e}")
            result["error"] = f"Error interno al leer el archivo: {str(e)}"
            return result

    def _detect_operation(self, columns: pd.Index) -> Tuple[Optional[str], Optional[List[str]]]:
        """
        Infiere la intención del usuario basándose en las columnas presentes.
        """
        columns_set = set(columns)

        # 1. CREAR USUARIOS
        if {"username", "email", "password"}.issubset(columns_set):
            required = self.REQUIRED_COLUMNS["CREATE_USER"]
            missing = required - columns_set
            # firstname y lastname son obligatorios en Moodle, no podemos ignorarlos
            if not missing:
                return "CREATE_USER", None
            return "CREATE_USER", list(missing)
        
        # 2. CREAR CURSOS
        if {"fullname", "category_id"}.issubset(columns_set):
            required = self.REQUIRED_COLUMNS["CREATE_COURSE"]
            missing = required - columns_set
            return "CREATE_COURSE", list(missing) if missing else None

        # 3. MATRICULACIÓN
        if {"username", "shortname"}.issubset(columns_set):
            required = self.REQUIRED_COLUMNS["ENROLL_USER"]
            missing = required - columns_set
            # 'role' es opcional (default student)
            if "role" in missing:
                missing.remove("role")
            return "ENROLL_USER", list(missing) if missing else None

        return None, None

    def dataframe_to_csv(self, df: pd.DataFrame) -> str:
        """Convierte el DF a string CSV para pasarlo a Celery/Redis."""
        return df.to_csv(index=False)

# Instancia global
processor = DataProcessor()