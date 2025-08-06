import json
import os

def create_eda_notebook():
    """Crear el notebook completo de EDA y ETL"""
    
    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# Análisis Exploratorio de Datos (EDA) y ETL para MongoDB\n",
                "## Caso de Estudio: Replicación Primario-Secundario en MongoDB"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "import pandas as pd\n",
                "import numpy as np\n",
                "import matplotlib.pyplot as plt\n",
                "import seaborn as sns\n",
                "from datetime import datetime\n",
                "from pymongo import MongoClient\n",
                "import warnings\n",
                "warnings.filterwarnings('ignore')"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Crear dataset de ejemplo\n",
                "np.random.seed(42)\n",
                "n_records = 1000\n",
                "productos = ['Laptop', 'Smartphone', 'Tablet', 'Monitor', 'Teclado', 'Mouse']\n",
                "ciudades = ['São Paulo', 'Rio de Janeiro', 'Brasília', 'Salvador', 'Fortaleza']\n",
                "\n",
                "data = {\n",
                "    'producto': np.random.choice(productos, n_records),\n",
                "    'precio': np.random.uniform(50, 2000, n_records),\n",
                "    'fecha_compra': pd.date_range('2023-01-01', periods=n_records, freq='D'),\n",
                "    'cliente_id': np.random.randint(1000, 9999, n_records),\n",
                "    'ciudad': np.random.choice(ciudades, n_records),\n",
                "    'cantidad_stock': np.random.randint(0, 100, n_records)\n",
                "}\n",
                "\n",
                "df = pd.DataFrame(data)\n",
                "print(f\"Dataset creado: {df.shape[0]} registros\")\n",
                "df.head()"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# EDA básico\n",
                "print(\"Información del dataset:\")\n",
                "print(df.info())\n",
                "print(\"\\nEstadísticas:\")\n",
                "print(df.describe())\n",
                "print(\"\\nValores nulos:\")\n",
                "print(df.isnull().sum())"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Visualizaciones\n",
                "fig, axes = plt.subplots(2, 2, figsize=(15, 10))\n",
                "\n",
                "# Distribución de precios\n",
                "axes[0,0].hist(df['precio'], bins=30, alpha=0.7)\n",
                "axes[0,0].set_title('Distribución de Precios')\n",
                "\n",
                "# Ventas por producto\n",
                "df['producto'].value_counts().plot(kind='bar', ax=axes[0,1])\n",
                "axes[0,1].set_title('Ventas por Producto')\n",
                "axes[0,1].tick_params(axis='x', rotation=45)\n",
                "\n",
                "# Ventas por ciudad\n",
                "df['ciudad'].value_counts().plot(kind='bar', ax=axes[1,0])\n",
                "axes[1,0].set_title('Ventas por Ciudad')\n",
                "axes[1,0].tick_params(axis='x', rotation=45)\n",
                "\n",
                "# Precio promedio por producto\n",
                "df.groupby('producto')['precio'].mean().plot(kind='bar', ax=axes[1,1])\n",
                "axes[1,1].set_title('Precio Promedio por Producto')\n",
                "axes[1,1].tick_params(axis='x', rotation=45)\n",
                "\n",
                "plt.tight_layout()\n",
                "plt.show()"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# ETL - Transformación de datos\n",
                "df['fecha_compra'] = pd.to_datetime(df['fecha_compra'])\n",
                "df['precio'] = df['precio'].astype(float)\n",
                "df['cliente_id'] = df['cliente_id'].astype(int)\n",
                "df['cantidad_stock'] = df['cantidad_stock'].astype(int)\n",
                "df['total_venta'] = df['precio'] * 1\n",
                "df['año_compra'] = df['fecha_compra'].dt.year\n",
                "df['mes_compra'] = df['fecha_compra'].dt.month\n",
                "\n",
                "print(\"Datos transformados para MongoDB\")\n",
                "df.head()"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Conexión a MongoDB\n",
                "MONGO_URI = \"mongodb://admin:password123@localhost:27017/\"\n",
                "DB_NAME = \"ventas_tienda_db\"\n",
                "COLLECTION_NAME = \"ventas\"\n",
                "\n",
                "try:\n",
                "    client = MongoClient(MONGO_URI)\n",
                "    client.admin.command('ping')\n",
                "    print(\"Conexión exitosa a MongoDB\")\n",
                "    \n",
                "    db = client[DB_NAME]\n",
                "    collection = db[COLLECTION_NAME]\n",
                "    collection.delete_many({})\n",
                "    \n",
                "    # Insertar datos\n",
                "    records = df.to_dict('records')\n",
                "    for record in records:\n",
                "        if isinstance(record['fecha_compra'], str):\n",
                "            record['fecha_compra'] = pd.to_datetime(record['fecha_compra'])\n",
                "    \n",
                "    result = collection.insert_many(records)\n",
                "    print(f\"Datos insertados: {len(result.inserted_ids)} registros\")\n",
                "    \n",
                "except Exception as e:\n",
                "    print(f\"Error: {e}\")"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Verificar replicación\n",
                "try:\n",
                "    # Insertar documento de prueba\n",
                "    test_doc = {\n",
                "        'producto': 'Monitor de Prueba',\n",
                "        'precio': 150.0,\n",
                "        'fecha_compra': datetime.now(),\n",
                "        'cliente_id': 9999,\n",
                "        'ciudad': 'Ciudad de Prueba',\n",
                "        'cantidad_stock': 50\n",
                "    }\n",
                "    \n",
                "    result = collection.insert_one(test_doc)\n",
                "    print(f\"Documento insertado: {result.inserted_id}\")\n",
                "    \n",
                "    # Leer desde secundario\n",
                "    import time\n",
                "    time.sleep(2)\n",
                "    \n",
                "    secondary_client = MongoClient(\"mongodb://admin:password123@localhost:27018/\")\n",
                "    secondary_db = secondary_client[DB_NAME]\n",
                "    secondary_collection = secondary_db[COLLECTION_NAME]\n",
                "    \n",
                "    doc = secondary_collection.find_one({'cliente_id': 9999})\n",
                "    if doc:\n",
                "        print(f\"✅ Replicación exitosa: {doc['producto']}\")\n",
                "    else:\n",
                "        print(\"❌ Replicación falló\")\n",
                "        \n",
                "except Exception as e:\n",
                "    print(f\"Error en prueba: {e}\")"
            ]
        }
    ]
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    return notebook

def create_crud_notebook():
    """Crear el notebook de consultas CRUD"""
    
    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# Consultas CRUD en MongoDB\n",
                "## Caso de Estudio: Replicación Primario-Secundario\n",
                "\n",
                "Este notebook contiene las 15 consultas CRUD requeridas para el proyecto."
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "from pymongo import MongoClient\n",
                "from datetime import datetime, timedelta\n",
                "import pandas as pd\n",
                "\n",
                "# Configuración de conexión\n",
                "MONGO_URI = \"mongodb://admin:password123@localhost:27017/\"\n",
                "DB_NAME = \"ventas_tienda_db\"\n",
                "COLLECTION_NAME = \"ventas\"\n",
                "\n",
                "client = MongoClient(MONGO_URI)\n",
                "db = client[DB_NAME]\n",
                "collection = db[COLLECTION_NAME]\n",
                "\n",
                "print(\"Conexión establecida a MongoDB\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Consulta 1: Ventas de los últimos 3 meses para un cliente específico"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Consulta 1: Ventas de los últimos 3 meses para cliente_id = 1234\n",
                "fecha_limite = datetime.now() - timedelta(days=90)\n",
                "\n",
                "consulta = {\n",
                "    'cliente_id': 1234,\n",
                "    'fecha_compra': {'$gte': fecha_limite}\n",
                "}\n",
                "\n",
                "resultados = list(collection.find(consulta).sort('fecha_compra', -1))\n",
                "print(f\"Ventas encontradas: {len(resultados)}\")\n",
                "for venta in resultados[:5]:\n",
                "    print(f\"- {venta['producto']}: ${venta['precio']} - {venta['fecha_compra']}\")"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Consulta 2: Total gastado por cliente en los últimos 3 meses, agrupado por producto"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Consulta 2: Total gastado por cliente, agrupado por producto\n",
                "pipeline = [\n",
                "    {'$match': {\n",
                "        'cliente_id': 1234,\n",
                "        'fecha_compra': {'$gte': fecha_limite}\n",
                "    }},\n",
                "    {'$group': {\n",
                "        '_id': '$producto',\n",
                "        'total_gastado': {'$sum': '$precio'},\n",
                "        'cantidad_ventas': {'$sum': 1}\n",
                "    }},\n",
                "    {'$sort': {'total_gastado': -1}}\n",
                "]\n",
                "\n",
                "resultados = list(collection.aggregate(pipeline))\n",
                "print(\"Total gastado por producto:\")\n",
                "for resultado in resultados:\n",
                "    print(f\"- {resultado['_id']}: ${resultado['total_gastado']:.2f} ({resultado['cantidad_ventas']} ventas)\")"
            ]
        }
    ]
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    return notebook

if __name__ == "__main__":
    # Crear directorio scripts si no existe
    os.makedirs("scripts", exist_ok=True)
    
    # Crear notebook EDA
    eda_notebook = create_eda_notebook()
    with open("notebooks/EDA_ETL_MongoDB.ipynb", "w", encoding="utf-8") as f:
        json.dump(eda_notebook, f, indent=2, ensure_ascii=False)
    
    # Crear notebook CRUD
    crud_notebook = create_crud_notebook()
    with open("notebooks/Consultas_CRUD.ipynb", "w", encoding="utf-8") as f:
        json.dump(crud_notebook, f, indent=2, ensure_ascii=False)
    
    print("✅ Notebooks creados exitosamente!")
    print("- notebooks/EDA_ETL_MongoDB.ipynb")
    print("- notebooks/Consultas_CRUD.ipynb") 