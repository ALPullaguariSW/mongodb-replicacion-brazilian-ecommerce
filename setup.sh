#!/bin/bash

# 🚀 MongoDB Replication Project - Setup Script
# Script de configuración automática para Linux/Mac

echo "🚀 Configurando Proyecto de Replicación MongoDB..."
echo "=================================================="

# Verificar si Docker está instalado
if ! command -v docker &> /dev/null; then
    echo "❌ Docker no está instalado. Por favor instala Docker Desktop."
    exit 1
fi

# Verificar si Python está instalado
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 no está instalado. Por favor instala Python 3.13+."
    exit 1
fi

echo "✅ Prerrequisitos verificados"

# Crear entorno virtual
echo "📦 Creando entorno virtual..."
python3 -m venv venv
source venv/bin/activate

# Instalar dependencias
echo "📥 Instalando dependencias..."
pip install --upgrade pip
pip install -r requirements.txt

# Levantar cluster MongoDB
echo "🐳 Levantando cluster MongoDB..."
cd docker
docker-compose up -d

# Esperar a que los contenedores estén listos
echo "⏳ Esperando a que los contenedores estén listos..."
sleep 30

# Verificar estado del replica set
echo "🔍 Verificando estado del replica set..."
docker exec -it mongo-primary mongosh --eval "rs.status()" --quiet

# Verificar datos en cada nodo
echo "📊 Verificando replicación de datos..."
echo "Primary: $(docker exec -it mongo-primary mongosh ecommerce_brazil --eval "db.ventas.countDocuments()" --quiet)"
echo "Secondary1: $(docker exec -it mongo-secondary1 mongosh ecommerce_brazil --eval "db.ventas.countDocuments()" --quiet)"
echo "Secondary2: $(docker exec -it mongo-secondary2 mongosh ecommerce_brazil --eval "db.ventas.countDocuments()" --quiet)"

cd ..

echo ""
echo "🎉 ¡Configuración completada!"
echo "=============================="
echo "📊 Para ejecutar el EDA y ETL:"
echo "   jupyter notebook notebooks/EDA_ETL_MongoDB.ipynb"
echo ""
echo "🔍 Para ejecutar las consultas CRUD:"
echo "   jupyter notebook notebooks/Consultas_CRUD.ipynb"
echo ""
echo "🧪 Para probar la resiliencia:"
echo "   jupyter notebook notebooks/Pruebas_Resiliencia_Replicacion.ipynb"
echo ""
echo "🌐 MongoDB disponible en:"
echo "   Primary: localhost:27020"
echo "   Secondary1: localhost:27021"
echo "   Secondary2: localhost:27022"
echo ""
echo "📖 Lee el README.md para más información" 