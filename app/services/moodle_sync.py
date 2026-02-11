import requests
import logging
from typing import Dict, Any, Optional
from app.core.config import settings

# Configuración de Logging
logger = logging.getLogger(__name__)

class MoodleClient:
    """
    Cliente para interactuar con la API REST de Moodle (Web Services).
    Versión Final: Incluye validación de contraseñas, sanitización de errores y verificación de categorías.
    """

    def __init__(self):
        self.api_url = settings.MOODLE_API_URL
        self.token = settings.MOODLE_API_TOKEN
        self.format = "json"

    def _send_request(self, function: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Envía la petición POST a Moodle.
        Centraliza el manejo de excepciones HTTP y errores lógicos de la API.
        """
        payload = {
            "wstoken": self.token,
            "wsfunction": function,
            "moodlewsrestformat": self.format,
        }
        payload.update(params)

        try:
            # Timeout de 30s
            response = requests.post(self.api_url, data=payload, timeout=30)
            response.raise_for_status() 
            
            data = response.json()

            # Detección de errores lógicos devueltos por Moodle (Exception handling)
            if isinstance(data, dict) and ("exception" in data or "debuginfo" in data):
                error_msg = data.get("message", "Error desconocido de Moodle")
                error_code = data.get("errorcode", "")
                
                logger.error(f"Error Moodle API ({function}) [{error_code}]: {error_msg}")
                return {"success": False, "error": error_msg, "code": error_code}

            return {"success": True, "data": data}

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de Conexión con Moodle: {e}")
            return {"success": False, "error": f"Error de conexión: {str(e)}"}
        except Exception as e:
            logger.exception(f"Error inesperado en cliente Moodle: {e}")
            return {"success": False, "error": f"Error interno: {str(e)}"}

    # =========================================================================
    # 1. GESTIÓN DE USUARIOS
    # =========================================================================

    def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Crea un usuario validando longitud de contraseña previamente."""
        password = str(user_data.get("password", ""))

        if len(password) < 8:
            return {"success": False, "error": "Contraseña insegura: Mínimo 8 caracteres requeridos."}

        params = {
            "users[0][username]": user_data.get("username"),
            "users[0][password]": password,
            "users[0][firstname]": user_data.get("firstname"),
            "users[0][lastname]": user_data.get("lastname"),
            "users[0][email]": user_data.get("email"),
            "users[0][auth]": "manual",
            "users[0][lang]": "es",
        }
        
        if "idnumber" in user_data:
            params["users[0][idnumber]"] = user_data["idnumber"]

        result = self._send_request("core_user_create_users", params)

        if not result["success"]:
            err_msg = result.get("error", "").lower()
            if "password" in err_msg and "policy" in err_msg:
                return {"success": False, "error": "La contraseña no cumple la política de seguridad (Mayús, Minús, Núm, Caracter esp)."}
            if "username" in err_msg and "already exists" in err_msg:
                return {"success": False, "error": "El usuario ya existe."}

        return result

    def get_user_id_by_username(self, username: str) -> Optional[int]:
        """Obtiene ID numérico de usuario."""
        params = {"field": "username", "values[0]": username}
        result = self._send_request("core_user_get_users_by_field", params)
        
        if result["success"] and result["data"]:
            return result["data"][0]["id"]
        return None

    # =========================================================================
    # 2. GESTIÓN DE CURSOS (Solución Problema 4)
    # =========================================================================

    def check_category_exists(self, category_id: int) -> bool:
        """
        Verifica si una categoría existe en Moodle.
        Usa 'core_course_get_categories' filtrando por ID.
        """
        params = {
            "criteria[0][key]": "id",
            "criteria[0][value]": category_id
        }
        result = self._send_request("core_course_get_categories", params)
        
        # Si la API responde OK y la lista 'data' tiene al menos 1 elemento, existe.
        if result["success"] and isinstance(result["data"], list) and len(result["data"]) > 0:
            return True
        return False

    def create_course(self, course_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crea un curso nuevo.
        CORRECCIÓN: Valida existencia de categoría antes de crear.
        """
        try:
            category_id = int(course_data.get("category_id", 0))
        except ValueError:
            return {"success": False, "error": "El ID de categoría debe ser un número entero."}

        # 1. Validación de Integridad (Solución Problema 4)
        if not self.check_category_exists(category_id):
            return {
                "success": False, 
                "error": f"La categoría con ID {category_id} no existe en Moodle. Cree la categoría primero o verifique el ID."
            }

        # 2. Creación del Curso
        params = {
            "courses[0][fullname]": course_data.get("fullname"),
            "courses[0][shortname]": course_data.get("shortname"),
            "courses[0][categoryid]": category_id, 
            "courses[0][visible]": 1,
            "courses[0][format]": "topics"
        }
        
        if "startdate" in course_data:
            params["courses[0][startdate]"] = course_data["startdate"]

        return self._send_request("core_course_create_courses", params)

    def get_course_id_by_shortname(self, shortname: str) -> Optional[int]:
        """Obtiene ID numérico de curso."""
        params = {"field": "shortname", "value": shortname}
        result = self._send_request("core_course_get_courses_by_field", params)
        
        if result["success"] and "courses" in result["data"] and result["data"]["courses"]:
            return result["data"]["courses"][0]["id"]
        return None

    # =========================================================================
    # 3. MATRICULACIÓN
    # =========================================================================

    def enroll_user(self, enrollment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Matricula usuario resolviendo IDs."""
        username = enrollment_data.get("username")
        shortname = enrollment_data.get("shortname")
        role_id = enrollment_data.get("role_id", 5)

        user_id = self.get_user_id_by_username(username)
        if not user_id:
            return {"success": False, "error": f"Usuario '{username}' no encontrado."}

        course_id = self.get_course_id_by_shortname(shortname)
        if not course_id:
            return {"success": False, "error": f"Curso '{shortname}' no encontrado."}

        params = {
            "enrolments[0][roleid]": role_id,
            "enrolments[0][userid]": user_id,
            "enrolments[0][courseid]": course_id
        }
        
        result = self._send_request("enrol_manual_enrol_users", params)
        
        if result["success"] and result["data"] is None:
            return {"success": True, "data": "Matriculado correctamente"}
            
        return result

# Instancia Global
moodle_client = MoodleClient()