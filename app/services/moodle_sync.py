import requests
import logging
from typing import Dict, Any, Optional, List
from app.core.config import settings

# Configuración de Logging
logger = logging.getLogger(__name__)

class MoodleClient:
    """
    Cliente para interactuar con la API REST de Moodle (Web Services).
    
    Características:
    - Moodle 3.9+ Compatible.
    - Manejo de Usuarios, Cursos, Matriculaciones.
    - Soporte para 'Template Courses' mediante importación diferida.
    - Validaciones de seguridad (contraseñas, categorías existentes).
    """

    def __init__(self):
        self.api_url = settings.MOODLE_API_URL
        self.token = settings.MOODLE_API_TOKEN
        self.format = "json"

    def _send_request(self, function: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Envía la petición POST a Moodle.
        Centraliza el manejo de tokens, formato JSON y captura de errores HTTP/Lógicos.
        """
        payload = {
            "wstoken": self.token,
            "wsfunction": function,
            "moodlewsrestformat": self.format,
        }
        payload.update(params)

        try:
            # Timeout de 30s para evitar bloqueos en operaciones pesadas (como importación)
            response = requests.post(self.api_url, data=payload, timeout=30)
            response.raise_for_status() 
            
            data = response.json()

            # Detección de errores lógicos devueltos por Moodle
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

    def get_user_id_by_username(self, username: str) -> Optional[int]:
        """Obtiene ID numérico de usuario dado su username."""
        params = {"field": "username", "values[0]": username}
        result = self._send_request("core_user_get_users_by_field", params)
        
        if result["success"] and result["data"]:
            return result["data"][0]["id"]
        return None

    def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crea un usuario validando longitud de contraseña previamente.
        """
        password = str(user_data.get("password", ""))

        # Validación local de seguridad (PDF Req: evitar contraseñas débiles)
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
                return {"success": False, "error": "La contraseña no cumple políticas (Mayús, Minús, Núm, Caracter esp)."}
            if "username" in err_msg and "already exists" in err_msg:
                return {"success": False, "error": "El usuario ya existe."}

        return result

    def delete_user(self, username: str) -> Dict[str, Any]:
        """Elimina un usuario del sistema."""
        user_id = self.get_user_id_by_username(username)
        if not user_id:
            return {"success": False, "error": f"Usuario '{username}' no encontrado."}

        params = {"userids[0]": user_id}
        result = self._send_request("core_user_delete_users", params)
        
        if result["success"] and result["data"] is None:
             return {"success": True, "data": f"Usuario '{username}' eliminado correctamente."}
        return result

    # =========================================================================
    # 2. GESTIÓN DE CURSOS
    # =========================================================================

    def get_course_id_by_shortname(self, shortname: str) -> Optional[int]:
        """Obtiene ID numérico de curso."""
        params = {"field": "shortname", "value": shortname}
        result = self._send_request("core_course_get_courses_by_field", params)
        
        if result["success"] and "courses" in result["data"] and result["data"]["courses"]:
            return result["data"]["courses"][0]["id"]
        return None

    def check_category_exists(self, category_id: int) -> bool:
        """Verifica si una categoría existe en Moodle."""
        params = {"criteria[0][key]": "id", "criteria[0][value]": category_id}
        result = self._send_request("core_course_get_categories", params)
        if result["success"] and isinstance(result["data"], list) and len(result["data"]) > 0:
            return True
        return False

    def create_course(self, course_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crea un curso nuevo validando la categoría primero.
        NOTA: No procesa 'templatecourse' aquí, eso se hace en import_course_content.
        """
        try:
            category_id = int(course_data.get("category_id") or 0)
        except ValueError:
            return {"success": False, "error": "El ID de categoría debe ser numérico."}

        # Validación Problema 4 (IDs inexistentes)
        if not self.check_category_exists(category_id):
            return {"success": False, "error": f"La categoría ID {category_id} no existe."}

        params = {
            "courses[0][fullname]": course_data.get("fullname"),
            "courses[0][shortname]": course_data.get("shortname"),
            "courses[0][categoryid]": category_id, 
            "courses[0][visible]": 1,
            "courses[0][format]": course_data.get("format", "topics"),
        }
        
        if "category_idnumber" in course_data:
             params["courses[0][idnumber]"] = course_data["category_idnumber"]

        return self._send_request("core_course_create_courses", params)

    def import_course_content(self, target_course_id: int, template_shortname: str) -> Dict[str, Any]:
        """
        NUEVO: Simula la funcionalidad 'templatecourse'.
        Importa contenido de una plantilla al curso destino.
        API: core_course_import_course
        """
        # 1. Obtener ID de la plantilla
        template_id = self.get_course_id_by_shortname(template_shortname)
        if not template_id:
            return {
                "success": False, 
                "error": f"Plantilla '{template_shortname}' no encontrada. Curso creado vacío."
            }

        # 2. Ejecutar importación
        params = {
            "importfrom": template_id,
            "importto": target_course_id,
            "deletecontent": 0  # 0 = No borrar (merge), 1 = Borrar destino antes
        }

        result = self._send_request("core_course_import_course", params)

        if result["success"]:
            # La API suele devolver null o vacio en éxito de importación
            return {"success": True, "data": f"Contenido importado desde '{template_shortname}'."}
        
        return result

    def update_course_visibility(self, shortname: str, visible: int) -> Dict[str, Any]:
        """Actualiza visibilidad (1=Ver, 0=Ocultar)."""
        course_id = self.get_course_id_by_shortname(shortname)
        if not course_id:
            return {"success": False, "error": f"Curso '{shortname}' no encontrado."}

        params = {
            "courses[0][id]": course_id,
            "courses[0][visible]": int(visible)
        }
        return self._send_request("core_course_update_courses", params)

    def delete_course(self, shortname: str) -> Dict[str, Any]:
        """Elimina un curso permanentemente."""
        course_id = self.get_course_id_by_shortname(shortname)
        if not course_id:
            return {"success": False, "error": f"Curso '{shortname}' no encontrado."}

        params = {"courseids[0]": course_id}
        return self._send_request("core_course_delete_courses", params)

    # =========================================================================
    # 3. MATRICULACIÓN
    # =========================================================================

    def enroll_user(self, enrollment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Matricula usuario resolviendo IDs y Roles.
        """
        username = enrollment_data.get("username")
        shortname = enrollment_data.get("shortname")
        
        # Mapeo de Roles (fallback a student si falla)
        role_input = enrollment_data.get("role", "student")
        role_map = {
            "manager": 1, "coursecreator": 2, "editingteacher": 3,
            "teacher": 4, "student": 5, "guest": 6
        }
        
        role_id = role_map.get(str(role_input).lower(), 5) 

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