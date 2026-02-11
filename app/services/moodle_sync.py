import requests
import os
import logging
from typing import Dict, Any, Optional

# Configuración de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MoodleClient:
    def __init__(self):
        # Carga credenciales desde variables de entorno
        self.url = os.getenv("MOODLE_API_URL")
        self.token = os.getenv("MOODLE_API_TOKEN")
        
        if not self.url or not self.token:
            logger.warning("Faltan variables de entorno MOODLE_API_URL o MOODLE_API_TOKEN")

    def _send_request(self, ws_function: str, params: Dict[str, Any], action_name: str, identifier: str) -> Dict[str, Any]:
        """
        Método central para enviar peticiones a Moodle.
        Maneja la autenticación y la detección de errores en la respuesta JSON.
        """
        try:
            # Parámetros base obligatorios
            payload = {
                "wstoken": self.token,
                "wsfunction": ws_function,
                "moodlewsrestformat": "json"
            }
            # Fusionar con los parámetros específicos de la función
            payload.update(params)

            response = requests.post(self.url, data=payload, timeout=30)
            response.raise_for_status() # Lanza error si es 404, 500, etc.
            
            data = response.json()

            # --- DETECCIÓN DE ERRORES DE LÓGICA DE MOODLE ---
            # Moodle devuelve 'exception' o 'errorcode' dentro del JSON si falla
            if isinstance(data, dict) and ('exception' in data or 'errorcode' in data):
                error_msg = data.get('message', 'Error desconocido de Moodle')
                logger.error(f"Fallo en {action_name} para {identifier}: {error_msg}")
                return {"success": False, "error": error_msg}

            # Caso especial: Crear usuario devuelve una lista, pero si falla a veces devuelve null
            if data is None:
                return {"success": False, "error": "Moodle devolvió respuesta vacía"}

            logger.info(f"Éxito: {action_name} - {identifier}")
            return {"success": True, "data": data}

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión en {action_name}: {str(e)}")
            return {"success": False, "error": f"Error de conexión: {str(e)}"}

    # ==========================================
    # FUNCIONES AUXILIARES (Lookups)
    # ==========================================

    def get_user_id(self, username: str) -> Optional[int]:
        """Busca el ID numérico de un usuario dado su username."""
        params = {
            "field": "username",
            "values[0]": username
        }
        res = self._send_request("core_user_get_users_by_field", params, "get_user_id", username)
        if res['success'] and res['data'] and len(res['data']) > 0:
            return res['data'][0]['id']
        return None

    def get_course_id(self, shortname: str) -> Optional[int]:
        """Busca el ID numérico de un curso dado su shortname."""
        params = {
            "field": "shortname",
            "value": shortname
        }
        res = self._send_request("core_course_get_courses_by_field", params, "get_course_id", shortname)
        if res['success'] and res['data'] and 'courses' in res['data'] and len(res['data']['courses']) > 0:
            return res['data']['courses'][0]['id']
        return None

    def get_category_id(self, idnumber: str) -> int:
        """
        Busca el ID de una categoría por su IDNUMBER. 
        Si no existe, retorna 1 (Miscelánea) por defecto para evitar fallos críticos.
        """
        params = {"criteria[0][key]": "idnumber", "criteria[0][value]": idnumber}
        res = self._send_request("core_course_get_categories", params, "get_category", idnumber)
        
        if res['success'] and res['data'] and len(res['data']) > 0:
            return res['data'][0]['id']
        
        logger.warning(f"Categoría {idnumber} no encontrada. Usando Categoría 1 por defecto.")
        return 1

    # ==========================================
    # OPERACIONES PRINCIPALES
    # ==========================================

    def create_course(self, course_data: Dict) -> Dict:
        """
        Crea un curso nuevo.
        Requiere: fullname, shortname, category_idnumber (opcional)
        """
        # Resolver ID de categoría
        cat_id = 1
        if 'category_idnumber' in course_data:
            cat_id = self.get_category_id(course_data['category_idnumber'])

        params = {
            "courses[0][fullname]": course_data.get('fullname'),
            "courses[0][shortname]": course_data.get('shortname'),
            "courses[0][categoryid]": cat_id,
            "courses[0][format]": course_data.get('format', 'topics'),
            "courses[0][visible]": 1
        }
        return self._send_request("core_course_create_courses", params, "Crear Curso", course_data.get('shortname'))

    def create_user(self, user_data: Dict) -> Dict:
        """
        Crea un usuario nuevo.
        Requiere: username, password, firstname, lastname, email
        """
        params = {
            "users[0][username]": user_data['username'],
            "users[0][password]": user_data.get('password', 'Moodle123!'), # Fallback password
            "users[0][firstname]": user_data['firstname'],
            "users[0][lastname]": user_data['lastname'],
            "users[0][email]": user_data['email'],
            "users[0][auth]": "manual"
        }
        return self._send_request("core_user_create_users", params, "Crear Usuario", user_data['username'])

    def enroll_user(self, data: Dict) -> Dict:
        """
        Matricula un usuario en un curso.
        Requiere: username, shortname (o course1), roleid (default 5=student)
        """
        # 1. Obtener IDs (Paso crítico)
        user_id = self.get_user_id(data.get('username'))
        course_id = self.get_course_id(data.get('shortname'))

        if not user_id:
            return {"success": False, "error": f"Usuario {data.get('username')} no encontrado"}
        if not course_id:
            return {"success": False, "error": f"Curso {data.get('shortname')} no encontrado"}

        # Rol: 5 es estudiante, 3 es profesor (Editing Teacher)
        role_id = 5 
        if str(data.get('role1', '')).lower() in ['teacher', 'profesor', 'docente', '3']:
            role_id = 3

        params = {
            "enrolments[0][roleid]": role_id,
            "enrolments[0][userid]": user_id,
            "enrolments[0][courseid]": course_id
        }
        
        return self._send_request("enrol_manual_enrol_users", params, "Matricular", f"{data.get('username')} en {data.get('shortname')}")

    def delete_course(self, shortname: str) -> Dict:
        """Elimina un curso."""
        course_id = self.get_course_id(shortname)
        if not course_id:
            return {"success": False, "error": f"Curso {shortname} no encontrado para eliminar"}

        params = {"courseids[0]": course_id}
        return self._send_request("core_course_delete_courses", params, "Eliminar Curso", shortname)

    def delete_user(self, username: str) -> Dict:
        """Elimina un usuario."""
        user_id = self.get_user_id(username)
        if not user_id:
            return {"success": False, "error": f"Usuario {username} no encontrado para eliminar"}

        params = {"userids[0]": user_id}
        return self._send_request("core_user_delete_users", params, "Eliminar Usuario", username)

    def update_course_visibility(self, shortname: str, visible: int) -> Dict:
        """
        Cambia la visibilidad de un curso.
        visible: 1 (mostrar) | 0 (ocultar)
        """
        course_id = self.get_course_id(shortname)
        if not course_id:
            return {"success": False, "error": f"Curso {shortname} no encontrado"}

        params = {
            "courses[0][id]": course_id,
            "courses[0][visible]": int(visible)
        }
        return self._send_request("core_course_update_courses", params, "Actualizar Visibilidad", shortname)

# Instancia Singleton para usar en otros módulos
moodle_client = MoodleClient()