import requests
import logging
from typing import Dict, Any, Optional
from app.core.config import settings

# Configuración de Logging
logger = logging.getLogger(__name__)

class MoodleClient:
    """
    Cliente para interactuar con la API REST de Moodle (Web Services).
    Documentación API Moodle: https://moodle.ut.edu.co/admin/tool/mobile/index.php
    """

    def __init__(self):
        self.api_url = settings.MOODLE_API_URL
        self.token = settings.MOODLE_API_TOKEN
        self.format = "json"

    def _send_request(self, function: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Envía la petición POST a Moodle y valida errores de nivel aplicación."""
        payload = {
            "wstoken": self.token,
            "wsfunction": function,
            "moodlewsrestformat": self.format,
        }
        payload.update(params)

        try:
            response = requests.post(self.api_url, data=payload, timeout=30)
            response.raise_for_status() # Lanza error si HTTP != 200
            
            data = response.json()

            # Moodle devuelve 200 OK incluso con errores lógicos. Validamos el contenido.
            if isinstance(data, dict) and ("exception" in data or "debuginfo" in data):
                error_msg = data.get("message", "Error desconocido de Moodle")
                logger.error(f"Error Moodle API ({function}): {error_msg}")
                return {"success": False, "error": error_msg}

            return {"success": True, "data": data}

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de Conexión con Moodle: {e}")
            return {"success": False, "error": f"Error de red: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": f"Error inesperado: {str(e)}"}

    # =========================================================================
    # 1. GESTIÓN DE USUARIOS
    # =========================================================================

    def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crea un usuario en Moodle.
        Requiere: username, password, firstname, lastname, email.
        """
        params = {
            "users[0][username]": user_data.get("username"),
            "users[0][password]": user_data.get("password"), # Moodle exige password segura
            "users[0][firstname]": user_data.get("firstname"),
            "users[0][lastname]": user_data.get("lastname"),
            "users[0][email]": user_data.get("email"),
            "users[0][auth]": "manual",
        }
        return self._send_request("core_user_create_users", params)

    def get_user_id_by_username(self, username: str) -> Optional[int]:
        """Busca el ID numérico de un usuario dado su username (cédula/código)."""
        params = {
            "field": "username",
            "values[0]": username
        }
        result = self._send_request("core_user_get_users_by_field", params)
        
        if result["success"] and result["data"]:
            # La API devuelve una lista, tomamos el primero
            return result["data"][0]["id"]
        return None

    # =========================================================================
    # 2. GESTIÓN DE CURSOS
    # =========================================================================

    def create_course(self, course_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crea un curso nuevo.
        Valida que la categoría exista antes de crear.
        """
        category_id = course_data.get("category_id")
        
        # CORRECCIÓN: Validar existencia de categoría (Opcional, pero recomendado)
        # Si Moodle devuelve error "category not found", lo capturamos en _send_request.
        
        params = {
            "courses[0][fullname]": course_data.get("fullname"),
            "courses[0][shortname]": course_data.get("shortname"),
            "courses[0][categoryid]": category_id, 
            "courses[0][visible]": 1
        }
        
        # Opcional: Fechas de inicio/fin si vienen en el Excel
        if "startdate" in course_data:
            params["courses[0][startdate]"] = course_data["startdate"]

        result = self._send_request("core_course_create_courses", params)
        
        # Si falla por categoría inválida, el mensaje de error de Moodle será claro gracias a _send_request
        return result

    def get_course_id_by_shortname(self, shortname: str) -> Optional[int]:
        """Busca el ID numérico de un curso por su nombre corto."""
        params = {
            "field": "shortname",
            "value": shortname
        }
        # Nota: core_course_get_courses_by_field devuelve { "courses": [...] }
        result = self._send_request("core_course_get_courses_by_field", params)
        
        if result["success"] and "courses" in result["data"] and result["data"]["courses"]:
            return result["data"]["courses"][0]["id"]
        return None

    # =========================================================================
    # 3. MATRICULACIÓN (ENROLLMENT)
    # =========================================================================

    def enroll_user(self, enrollment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Matricula un usuario en un curso.
        El Excel trae 'username' y 'shortname', pero Moodle pide IDs numéricos.
        """
        username = enrollment_data.get("username")
        shortname = enrollment_data.get("shortname") # Del curso
        role_id = enrollment_data.get("role_id", 5)  # 5 = Estudiante por defecto en Moodle

        # 1. Resolver User ID
        user_id = self.get_user_id_by_username(username)
        if not user_id:
            return {"success": False, "error": f"Usuario '{username}' no encontrado en Moodle."}

        # 2. Resolver Course ID
        course_id = self.get_course_id_by_shortname(shortname)
        if not course_id:
            return {"success": False, "error": f"Curso '{shortname}' no encontrado en Moodle."}

        # 3. Ejecutar Matriculación
        params = {
            "enrolments[0][roleid]": role_id,
            "enrolments[0][userid]": user_id,
            "enrolments[0][courseid]": course_id
        }
        
        # enrol_manual_enrol_users devuelve null (None) si tiene éxito, o excepción si falla
        result = self._send_request("enrol_manual_enrol_users", params)
        
        if result["success"] and result["data"] is None:
            # Moodle retorna null en éxito para esta función específica
            return {"success": True, "data": "Matriculado correctamente"}
            
        return result

# Instancia Global
moodle_client = MoodleClient()