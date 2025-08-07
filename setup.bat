@echo off
REM 🚀 MongoDB Replication Project - Setup Script
REM Script de configuración automática para Windows

echo 🚀 Configurando Proyecto de Replicación MongoDB...
echo ==================================================

REM Verificar si Docker está instalado
docker --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker no está instalado. Por favor instala Docker Desktop.
    pause
    exit /b 1
)

REM Verificar si Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python no está instalado. Por favor instala Python 3.13+.
    pause
    exit /b 1
)

echo ✅ Prerrequisitos verificados

REM Crear entorno virtual
echo 📦 Creando entorno virtual...
python -m venv venv
call venv\Scripts\activate.bat

REM Instalar dependencias
echo 📥 Instalando dependencias...
python -m pip install --upgrade pip
pip install -r requirements.txt

REM Levantar cluster MongoDB
echo 🐳 Levantando cluster MongoDB...
cd docker
docker-compose up -d

REM Esperar a que los contenedores estén listos
echo ⏳ Esperando a que los contenedores estén listos...
timeout /t 30 /nobreak >nul

REM Verificar estado del replica set
echo 🔍 Verificando estado del replica set...
docker exec -it mongo-primary mongosh --eval "rs.status()" --quiet

REM Verificar datos en cada nodo
echo 📊 Verificando replicación de datos...
for /f "tokens=*" %%i in ('docker exec -it mongo-primary mongosh ecommerce_brazil --eval "db.ventas.countDocuments()" --quiet') do echo Primary: %%i
for /f "tokens=*" %%i in ('docker exec -it mongo-secondary1 mongosh ecommerce_brazil --eval "db.ventas.countDocuments()" --quiet') do echo Secondary1: %%i
for /f "tokens=*" %%i in ('docker exec -it mongo-secondary2 mongosh ecommerce_brazil --eval "db.ventas.countDocuments()" --quiet') do echo Secondary2: %%i

cd ..

echo.
echo 🎉 ¡Configuración completada!
echo ==============================
echo 📊 Para ejecutar el EDA y ETL:
echo    jupyter notebook notebooks/EDA_ETL_MongoDB.ipynb
echo.
echo 🔍 Para ejecutar las consultas CRUD:
echo    jupyter notebook notebooks/Consultas_CRUD.ipynb
echo.
echo 🧪 Para probar la resiliencia:
echo    jupyter notebook notebooks/Pruebas_Resiliencia_Replicacion.ipynb
echo.
echo 🌐 MongoDB disponible en:
echo    Primary: localhost:27020
echo    Secondary1: localhost:27021
echo    Secondary2: localhost:27022
echo.
echo 📖 Lee el README.md para más información
pause 