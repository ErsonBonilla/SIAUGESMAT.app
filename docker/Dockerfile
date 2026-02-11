#Usamos la imagen oficial de Python 3.12 (versión slim para menor peso)
FROM python:3.12-slim

# Variables de entorno para optimizar Python en Docker
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Nuevo en 3.12: Permite instalar librerías globales en el contenedor sin advertencias
ENV PIP_BREAK_SYSTEM_PACKAGES=1

WORKDIR /app

# Instalar dependencias del sistema
# Se requiere libpq-dev para compilar psycopg2 (el driver de Postgres)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar requerimientos
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY . .

# Exponer el puerto
EXPOSE 8080

# Comando por defecto
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]