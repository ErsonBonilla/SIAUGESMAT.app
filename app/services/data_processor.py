import pandas as pd
import io
import re
from typing import Dict, Any, List, Tuple, Optional
import logging

# Configuración de logging
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Clase encargada de ingerir, limpiar y TRANSFORMAR los datos.
    Implementa la lógica de negocio de la Universidad del Tolima (PDF)
    para convertir datos académicos en estructuras compatibles con Moodle.
    """

    # Definición de conjuntos de columnas para detección de operaciones
    # Estas son las columnas que ESPERAMOS encontrar o GENERAR.
    REQUIRED_COLUMNS = {
        "CREATE_USER": {"username", "firstname", "lastname", "email", "password"},
        "ENROLL_USER": {"username", "shortname", "role"}, 
        # CREATE_COURSE puede venir de dos formas:
        # 1. Datos Académicos Crudos (nombre_cat, cod_programa...) -> Se transforman
        # 2. Datos Moodle Listos (fullname, shortname...) -> Se pasan directo
        "CREATE_COURSE_RAW": {"nombre_cat", "cod_programa", "cod_curso", "semestre", "grupo", "nombre_curso"},
        "CREATE_COURSE_MOODLE": {"fullname", "shortname", "category_idnumber"}
    }

    def _clean_general_text(self, text: Any) -> str:
        """Limpieza estándar: Strings sin espacios al inicio/final."""
        if pd.isna(text) or text is None:
            return ""
        return str(text).strip()

    def _clean_username(self, text: Any) -> str:
        """
        Sanitización ESTRICTA para 'username'.
        Moodle prohíbe espacios y mayúsculas.
        Solo permite: letras minúsculas, números, -, _, ., @
        """
        if pd.isna(text) or text is None:
            return ""
        s = str(text).lower().strip()
        # Eliminar cualquier caracter que no sea permitido
        s = re.sub(r'[^a-z0-9\.\-\@_]', '', s)
        return s

    def _get_cat_prefix(self, cat_name: str) -> str:
        """
        Regla de Negocio PDF: 
        "para 'APARTADO' se cambiará a URABA y se tomará sus tres primeras letras (URA)".
        Para los demás, toma las 3 primeras letras.
        """
        name = str(cat_name).upper().strip()
        if "APARTADO" in name:
            return "URA" 
        
        # Eliminar caracteres no alfabéticos para asegurar 3 letras limpias
        clean_name = re.sub(r'[^A-Z]', '', name)
        return clean_name[:3]

    def _format_program_code(self, code: Any) -> str:
        """
        Regla de Negocio PDF:
        "Como excel toma esta columna como números enteros borra el cero... se agregaría el '0' al principio"
        """
        try:
            # Convertir a float primero para manejar "4.0", luego a int, luego string
            return str(int(float(code))).zfill(2)
        except (ValueError, TypeError):
            return str(code).zfill(2)

    def _construct_moodle_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        MOTOR DE TRANSFORMACIÓN.
        Aplica las fórmulas del PDF para crear 'shortname', 'fullname' y 'templatecourse'.
        """
        # Verificamos si tenemos las columnas académicas necesarias para calcular
        raw_cols = self.REQUIRED_COLUMNS["CREATE_COURSE_RAW"]
        
        # Intersección para ver si están (casi) todas. 
        # Usamos issubset permitiendo flexibilidad o chequeo estricto según prefieras.
        if raw_cols.issubset(set(df.columns)):
            
            logger.info("Datos académicos detectados. Generando campos Moodle calculados...")

            # 1. SHORTNAME
            # Fórmula: PrefixCAT + CodProg + CodCurso + _s + Semestre + Grupo
            # Ejemplo: URA04123_s101
            df['shortname'] = df.apply(lambda row: (
                f"{self._get_cat_prefix(row.get('nombre_cat', ''))}"
                f"{self._format_program_code(row.get('cod_programa', '00'))}"
                f"{str(row.get('cod_curso', '')).strip()}"
                f"_s{str(row.get('semestre', '')).strip()}"
                f"{str(row.get('grupo', '')).strip()}"
            ), axis=1)

            # 2. FULLNAME
            # Fórmula: NOMBRE CURSO + " - Grupo " + GRUPO
            df['fullname'] = df.apply(lambda row: (
                f"{str(row.get('nombre_curso', '')).strip()} - Grupo {str(row.get('grupo', '')).strip()}"
            ), axis=1)

            # 3. CATEGORY IDNUMBER
            # Fórmula: PrefixCAT + "_" + CodProg + "_s" + Semestre
            df['category_idnumber'] = df.apply(lambda row: (
                f"{self._get_cat_prefix(row.get('nombre_cat', ''))}_"
                f"{self._format_program_code(row.get('cod_programa', '00'))}_"
                f"s{str(row.get('semestre', '')).strip()}"
            ), axis=1)

            # 4. FORMATO
            df['format'] = 'onetopic'
            
            # 5. TEMPLATE COURSE (SOLUCIÓN PUNTO 4)
            # Esta columna le dice al Worker qué curso copiar.
            # Fórmula: PORTAFOLIO + CodProg + _ + CodCurso + s + Semestre
            # Ejemplo: PORTAFOLIO04_123s1
            df['templatecourse'] = df.apply(lambda row: (
                f"PORTAFOLIO"
                f"{self._format_program_code(row.get('cod_programa', '00'))}_"
                f"{str(row.get('cod_curso', '')).strip()}s"
                f"{str(row.get('semestre', '')).strip()}"
            ), axis=1)

        # Normalización de flags (Delete y Visibility)
        if 'visible' in df.columns:
             df['visible'] = pd.to_numeric(df['visible'], errors='coerce').fillna(1).astype(int)
        
        if 'delete' in df.columns:
             df['delete'] = pd.to_numeric(df['delete'], errors='coerce').fillna(0).astype(int)

        return df

    def analyze_file(self, file_content: bytes) -> Dict[str, Any]:
        """
        Pipeline principal: Lectura -> Limpieza -> Transformación -> Validación.
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
            # 1. Lectura del archivo (Excel o CSV)
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception:
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='latin-1')

            # 2. Normalización de encabezados
            # Todo a minúscula, sin espacios, sin puntos, sin guiones.
            # "COD. PROGRAMA" -> "cod_programa"
            df.columns = [
                str(col).strip().lower()
                .replace(" ", "_").replace(".", "").replace("-", "") 
                for col in df.columns
            ]

            # 3. Limpieza de Texto General
            df = df.map(self._clean_general_text)

            # 4. Limpieza Específica de Username (si existe)
            if 'username' in df.columns:
                df['username'] = df['username'].map(self._clean_username)

            # 5. TRANSFORMACIÓN (La magia ocurre aquí)
            # Calculamos shortname, templatecourse, etc.
            df = self._construct_moodle_fields(df)

            # 6. Eliminar filas vacías
            df.replace("", float("nan"), inplace=True)
            df.dropna(how='all', inplace=True)
            df.fillna("", inplace=True)

            if df.empty:
                result["error"] = "El archivo está vacío o no contiene datos válidos."
                return result

            # 7. Detectar Operación
            operation, missing = self._detect_operation(df.columns)
            
            if not operation:
                result["error"] = (
                    f"No se pudo determinar la operación. "
                    f"Encabezados encontrados: {list(df.columns)}. "
                    f"Para cursos asegúrese de tener: 'nombre_cat', 'cod_programa', 'cod_curso'..."
                )
                return result

            if missing:
                result["error"] = f"Para la operación {operation}, faltan las columnas: {missing}"
                return result

            # 8. Retorno Exitoso
            result["valid"] = True
            result["operation"] = operation
            result["dataframe"] = df
            result["summary"] = f"Se detectaron {len(df)} registros para: {operation}"
            # Vista previa para la UI
            result["preview"] = df.head(5).to_dict(orient='records')
            
            return result

        except Exception as e:
            logger.error(f"Error procesando archivo: {e}")
            result["error"] = f"Error interno de procesamiento: {str(e)}"
            return result

    def _detect_operation(self, columns: pd.Index) -> Tuple[Optional[str], Optional[List[str]]]:
        """Infiere la operación basada en las columnas presentes (calculadas o crudas)."""
        columns_set = set(columns)

        # A. CREAR CURSOS (Ya sea calculados o directos)
        # Si logramos calcular shortname y fullname, estamos listos para crear cursos
        if {"shortname", "fullname"}.issubset(columns_set):
             return "CREATE_COURSE", None
        
        # B. MATRICULACIÓN
        if {"username", "shortname"}.issubset(columns_set):
            # Role es opcional (default editingteacher/student), no lo hacemos obligatorio aquí
            return "ENROLL_USER", None
            
        # C. CREAR USUARIOS
        if {"username", "firstname", "lastname", "email", "password"}.issubset(columns_set):
            return "CREATE_USER", None

        # D. ELIMINAR CURSOS
        if {"shortname", "delete"}.issubset(columns_set):
            return "DELETE_COURSE", None
        
        # E. ELIMINAR USUARIOS
        if {"username", "delete"}.issubset(columns_set):
            return "DELETE_USER", None
        
        # F. VISIBILIDAD
        if {"shortname", "visible"}.issubset(columns_set):
            return "UPDATE_VISIBILITY", None

        return None, None

    def dataframe_to_csv(self, df: pd.DataFrame) -> str:
        """Serializa el DF para pasarlo a Celery/Redis."""
        return df.to_csv(index=False)

# Instancia global
processor = DataProcessor()