# 1. Imagen base oficial de Python 3.12 (versión slim para producción ligera)
FROM python:3.12-slim

# 2. Configuración de variables de entorno
# PYTHONDONTWRITEBYTECODE: Evita la creación de archivos .pyc (innecesarios en contenedores)
# PYTHONUNBUFFERED: Fuerza a que los logs salgan inmediatamente a la consola (vital para ver errores en tiempo real)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# 3. Directorio de trabajo
WORKDIR /app

# 4. Instalación de dependencias del sistema
# - netcat-openbsd: Necesario para el script 'start.sh' (wait_for_db)
# - libpq-dev: Librerías necesarias para el driver de PostgreSQL
# - build-essential & gcc: Necesarios para compilar ciertas librerías de Python si no hay wheels disponibles
# - curl: Útil para healthchecks internos
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    libpq-dev \
    gcc \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 5. Gestión de dependencias de Python
# Copiamos solo el requirements.txt primero para aprovechar la caché de Docker
COPY requirements.txt .

# Actualizamos pip e instalamos las dependencias
# Nota: 'setuptools' y 'wheel' son obligatorios en Python 3.12 para ciertas compilaciones
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# 6. Copia del código fuente
# Copiamos el resto de la aplicación
COPY . .

# 7. Preparación del script de arranque y Permisos
# Ajustamos los permisos del script y aseguramos el formato de línea Unix (LF)
RUN chmod +x app/docker/start.sh && \
    sed -i 's/\r$//' app/docker/start.sh

# 8. Configuración de Seguridad (Usuario No-Root)
# Creamos un usuario de sistema llamado 'siauser' y le damos propiedad sobre /app
RUN useradd -m -s /bin/bash siauser && \
    chown -R siauser:siauser /app

# Cambiamos al usuario seguro. A partir de aquí, nada corre como root.
USER siauser

# 9. Exponer puerto (Informativo, útil para documentación)
EXPOSE 8080

# 10. Punto de entrada
# Usamos ENTRYPOINT para que el script reciba las señales del sistema (SIGTERM) correctamente
ENTRYPOINT ["/app/app/docker/start.sh"]