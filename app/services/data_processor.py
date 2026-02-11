import pandas as pd
import io
import re
from typing import Dict, Any, List, Tuple, Optional
import logging

# Configuración de logging
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Clase encargada de ingerir, limpiar y TRANSFORMAR los datos académicos
    en formatos compatibles con la API de Moodle según las reglas de negocio de la UT.
    """

    # Columnas esperadas en el Excel Académico (Entrada Cruda)
    ACADEMIC_COLUMNS = {
        "CREATE_COURSE": {"nombre_cat", "cod_programa", "cod_curso", "semestre", "grupo", "nombre_curso"},
        # Nota: El PDF menciona 'shortname' y 'delete' para eliminar, asumimos que para borrar
        # el usuario podría subir directamente el shortname o la data académica para reconstruirlo.
    }

    # Columnas Técnicas de Moodle (Salida Procesada)
    MOODLE_COLUMNS = {
        "CREATE_USER": {"username", "firstname", "lastname", "email", "password"},
        "ENROLL_USER": {"username", "shortname", "role"}, 
        "CREATE_COURSE": {"fullname", "shortname", "category_idnumber", "format"}
    }

    def _clean_general_text(self, text: Any) -> str:
        """Limpieza estándar: Strings, sin espacios extremos."""
        if pd.isna(text) or text is None:
            return ""
        return str(text).strip()

    def _clean_username(self, text: Any) -> str:
        """
        Sanitización ESTRICTA para 'username'[cite: 114].
        Solo letras minúsculas, números, guion, punto, @.
        """
        if pd.isna(text) or text is None:
            return ""
        s = str(text).lower().strip()
        s = re.sub(r'[^a-z0-9\.\-\@_]', '', s)
        return s

    def _get_cat_prefix(self, cat_name: str) -> str:
        """
        Obtiene las 3 primeras letras del CAT.
        Regla de Negocio[cite: 85, 90]: 
        "para 'APARTADO' se cambiará a URABA y se tomará sus tres primeras letras".
        """
        name = str(cat_name).upper().strip()
        if "APARTADO" in name:
            return "URA" # URABA -> URA
        
        # Eliminar espacios y tomar los primeros 3 caracteres
        clean_name = re.sub(r'[^A-Z]', '', name)
        return clean_name[:3]

    def _format_program_code(self, code: Any) -> str:
        """
        Regla de Negocio[cite: 86, 91]:
        "Como excel toma esta columna como números enteros borra el cero... se agregaría el '0' al principio"
        Asumimos programas de 2 dígitos.
        """
        try:
            # Convertir a entero primero para quitar decimales (.0) si existen, luego a string zfill
            return str(int(float(code))).zfill(2)
        except (ValueError, TypeError):
            return str(code).zfill(2)

    def _construct_moodle_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica las fórmulas del PDF para transformar columnas académicas en campos de Moodle.
        """
        # ---------------------------------------------------------
        # LÓGICA DE CREACIÓN DE CURSOS (Transformación Masiva)
        # ---------------------------------------------------------
        if {'nombre_cat', 'cod_programa', 'cod_curso', 'grupo'}.issubset(df.columns):
            
            logger.info("Detectadas columnas académicas. Aplicando fórmulas de transformación UT.")

            # 1. SHORTNAME 
            # Fórmula: PrefixCAT + CodProg + CodCurso + _s + Semestre + Grupo
            # Nota: El PDF dice "+ _s + SEMESTRE". Interpretamos "_s" como separador literal.
            df['shortname'] = df.apply(lambda row: (
                f"{self._get_cat_prefix(row.get('nombre_cat', ''))}"
                f"{self._format_program_code(row.get('cod_programa', '00'))}"
                f"{str(row.get('cod_curso', '')).strip()}"
                f"_s{str(row.get('semestre', '')).strip()}" # Interpretación de "_s" + SEMESTRE
                f"{str(row.get('grupo', '')).strip()}"      # Concatenación directa del grupo
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

            # 4. FORMAT 
            # "format = 'onetopic'"
            df['format'] = 'onetopic'
            
            # 5. TEMPLATE COURSE [cite: 93-97]
            # Lógica para definir qué plantilla usar (opcional, se construye para referencia)
            # templatecourse = PORTAFOLIO + CodProg + _ + CodCurso + s + Semestre
            df['templatecourse'] = df.apply(lambda row: (
                f"PORTAFOLIO"
                f"{self._format_program_code(row.get('cod_programa', '00'))}_"
                f"{str(row.get('cod_curso', '')).strip()}s"
                f"{str(row.get('semestre', '')).strip()}"
            ), axis=1)

        # ---------------------------------------------------------
        # LÓGICA DE VISIBILIDAD Y BORRADO [cite: 104-109, 115-117]
        # ---------------------------------------------------------
        if 'visible' in df.columns:
             # Asegurar que sea 1 o 0
             df['visible'] = pd.to_numeric(df['visible'], errors='coerce').fillna(1).astype(int)
        
        if 'delete' in df.columns:
             # Asegurar que sea 1 o 0
             df['delete'] = pd.to_numeric(df['delete'], errors='coerce').fillna(0).astype(int)

        return df

    def analyze_file(self, file_content: bytes) -> Dict[str, Any]:
        """
        Pipeline principal de procesamiento.
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
            # 1. Lectura del archivo
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception:
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='latin-1')

            # 2. Normalización de encabezados
            # Convertimos a minúsculas y quitamos espacios y caracteres especiales
            # Ej: "COD. PROGRAMA" -> "cod_programa"
            df.columns = [
                str(col).strip().lower()
                .replace(" ", "_").replace(".", "").replace("-", "") 
                for col in df.columns
            ]

            # 3. Limpieza básica
            df = df.map(self._clean_general_text)

            # 4. Sanitización de Username (Si existe)
            if 'username' in df.columns:
                df['username'] = df['username'].map(self._clean_username)

            # 5. TRANSFORMACIÓN DE DATOS (LÓGICA UT)
            # Aquí aplicamos las fórmulas del PDF para generar shortname, fullname, etc.
            df = self._construct_moodle_fields(df)

            # 6. Eliminar filas vacías
            df.replace("", float("nan"), inplace=True)
            df.dropna(how='all', inplace=True)
            df.fillna("", inplace=True)

            if df.empty:
                result["error"] = "El archivo está vacío."
                return result

            # 7. Detectar Operación
            operation, missing = self._detect_operation(df.columns)
            
            if not operation:
                result["error"] = (
                    f"No se pudo detectar la operación. "
                    f"Encabezados detectados: {list(df.columns)}. "
                    f"Para crear cursos asegúrese de incluir: nombre_cat, cod_programa, cod_curso, semestre, grupo."
                )
                return result

            if missing:
                result["error"] = f"Para {operation}, faltan columnas: {missing}"
                return result

            # 8. Éxito
            result["valid"] = True
            result["operation"] = operation
            result["dataframe"] = df
            result["summary"] = f"Se procesarán {len(df)} registros para: {operation}"
            result["preview"] = df.head(5).to_dict(orient='records')
            
            return result

        except Exception as e:
            logger.error(f"Error procesando archivo: {e}")
            result["error"] = f"Error interno: {str(e)}"
            return result

    def _detect_operation(self, columns: pd.Index) -> Tuple[Optional[str], Optional[List[str]]]:
        columns_set = set(columns)

        # A. CREAR CURSOS (Desde Datos Académicos o Directos)
        # Si ya calculamos 'shortname' y 'fullname' en _construct_moodle_fields, validamos contra esos.
        if {"shortname", "fullname", "category_idnumber"}.issubset(columns_set):
             return "CREATE_COURSE", None
        
        # B. MATRICULACIÓN [cite: 98]
        if {"username", "shortname"}.issubset(columns_set):
            # role es opcional (default editingteacher o student)
            return "ENROLL_USER", None
            
        # C. CREAR USUARIOS [cite: 112]
        if {"username", "firstname", "lastname", "email", "password"}.issubset(columns_set):
            return "CREATE_USER", None

        # D. BORRAR/VISIBILIDAD (Operaciones especiales)
        if {"shortname", "delete"}.issubset(columns_set):
            return "DELETE_COURSE", None
        
        if {"shortname", "visible"}.issubset(columns_set):
            return "UPDATE_VISIBILITY", None

        return None, None

    def dataframe_to_csv(self, df: pd.DataFrame) -> str:
        return df.to_csv(index=False)

# Instancia global
processor = DataProcessor()