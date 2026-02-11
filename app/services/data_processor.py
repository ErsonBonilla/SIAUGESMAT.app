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

    REQUIRED_COLUMNS = {
        "CREATE_USER": {"username", "firstname", "lastname", "email", "password"},
        "ENROLL_USER": {"username", "shortname", "role"}, 
        "CREATE_COURSE_RAW": {"nombre_cat", "cod_programa", "cod_curso", "semestre", "grupo", "nombre_curso"},
        "CREATE_COURSE_MOODLE": {"fullname", "shortname", "category_idnumber"}
    }

    def _clean_general_text(self, text: Any) -> str:
        if pd.isna(text) or text is None:
            return ""
        return str(text).strip()

    def _clean_username(self, text: Any) -> str:
        if pd.isna(text) or text is None:
            return ""
        s = str(text).lower().strip()
        s = re.sub(r'[^a-z0-9\.\-\@_]', '', s)
        return s

    def _get_cat_prefix(self, cat_name: str) -> str:
        name = str(cat_name).upper().strip()
        if "APARTADO" in name:
            return "URA" 
        clean_name = re.sub(r'[^A-Z]', '', name)
        return clean_name[:3]

    def _format_program_code(self, code: Any) -> str:
        try:
            return str(int(float(code))).zfill(2)
        except (ValueError, TypeError):
            return str(code).zfill(2)

    def _generate_template_course(self, row: pd.Series) -> str:
        """
        Genera el nombre de la plantilla del curso basándose en las reglas del PDF.
        Si faltan los datos requeridos, aplica el fallback ('FC2025A').
        """
        cod_prog = str(row.get('cod_programa', '')).strip()
        cod_curso = str(row.get('cod_curso', '')).strip()
        semestre = str(row.get('semestre', '')).strip()

        # Condición de Fallback: Si no hay datos suficientes para armar la plantilla
        if not cod_prog or not cod_curso or not semestre or cod_prog == "nan" or cod_curso == "nan":
            return "FC2025A"

        cod_prog_fmt = self._format_program_code(cod_prog)
        return f"PORTAFOLIO{cod_prog_fmt}_{cod_curso}s{semestre}"

    def _construct_moodle_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """MOTOR DE TRANSFORMACIÓN."""
        raw_cols = self.REQUIRED_COLUMNS["CREATE_COURSE_RAW"]
        
        if raw_cols.issubset(set(df.columns)):
            logger.info("Datos académicos detectados. Generando campos Moodle calculados...")

            # 1. SHORTNAME (Refactorizado con 'G-' para el grupo)
            df['shortname'] = df.apply(lambda row: (
                f"{self._get_cat_prefix(row.get('nombre_cat', ''))}"
                f"{self._format_program_code(row.get('cod_programa', '00'))}"
                f"{str(row.get('cod_curso', '')).strip()}"
                f"_s{str(row.get('semestre', '')).strip()}"
                f"G-{str(row.get('grupo', '')).strip()}"
            ), axis=1)

            # 2. FULLNAME
            df['fullname'] = df.apply(lambda row: (
                f"{str(row.get('nombre_curso', '')).strip()} - Grupo {str(row.get('grupo', '')).strip()}"
            ), axis=1)

            # 3. CATEGORY IDNUMBER
            df['category_idnumber'] = df.apply(lambda row: (
                f"{self._get_cat_prefix(row.get('nombre_cat', ''))}_"
                f"{self._format_program_code(row.get('cod_programa', '00'))}_"
                f"s{str(row.get('semestre', '')).strip()}"
            ), axis=1)

            # 4. FORMATO
            df['format'] = 'onetopic'
            
            # 5. TEMPLATE COURSE (Refactorizado con lógica de Fallback)
            df['templatecourse'] = df.apply(self._generate_template_course, axis=1)

        if 'visible' in df.columns:
             df['visible'] = pd.to_numeric(df['visible'], errors='coerce').fillna(1).astype(int)
        
        if 'delete' in df.columns:
             df['delete'] = pd.to_numeric(df['delete'], errors='coerce').fillna(0).astype(int)

        return df

    def analyze_file(self, file_content: bytes) -> Dict[str, Any]:
        result = {
            "valid": False,
            "operation": None,
            "dataframe": None,
            "preview": [],
            "error": None,
            "summary": ""
        }

        try:
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception:
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='latin-1')

            df.columns = [
                str(col).strip().lower()
                .replace(" ", "_").replace(".", "").replace("-", "") 
                for col in df.columns
            ]

            df = df.map(self._clean_general_text)

            if 'username' in df.columns:
                df['username'] = df['username'].map(self._clean_username)

            df = self._construct_moodle_fields(df)

            df.replace("", float("nan"), inplace=True)
            df.dropna(how='all', inplace=True)
            df.fillna("", inplace=True)

            if df.empty:
                result["error"] = "El archivo está vacío o no contiene datos válidos."
                return result

            operation, missing = self._detect_operation(df.columns)
            
            if not operation:
                result["error"] = f"No se pudo determinar la operación. Encabezados: {list(df.columns)}."
                return result

            if missing:
                result["error"] = f"Para la operación {operation}, faltan las columnas: {missing}"
                return result

            result["valid"] = True
            result["operation"] = operation
            result["dataframe"] = df
            result["summary"] = f"Se detectaron {len(df)} registros para: {operation}"
            result["preview"] = df.head(5).to_dict(orient='records')
            
            return result

        except Exception as e:
            logger.error(f"Error procesando archivo: {e}")
            result["error"] = f"Error interno de procesamiento: {str(e)}"
            return result

    def _detect_operation(self, columns: pd.Index) -> Tuple[Optional[str], Optional[List[str]]]:
        columns_set = set(columns)
        if {"shortname", "fullname"}.issubset(columns_set):
             return "CREATE_COURSE", None
        if {"username", "shortname"}.issubset(columns_set):
            return "ENROLL_USER", None
        if {"username", "firstname", "lastname", "email", "password"}.issubset(columns_set):
            return "CREATE_USER", None
        if {"shortname", "delete"}.issubset(columns_set):
            return "DELETE_COURSE", None
        if {"username", "delete"}.issubset(columns_set):
            return "DELETE_USER", None
        if {"shortname", "visible"}.issubset(columns_set):
            return "UPDATE_VISIBILITY", None
        return None, None

    def dataframe_to_csv(self, df: pd.DataFrame) -> str:
        return df.to_csv(index=False)

processor = DataProcessor()