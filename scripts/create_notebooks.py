#!/usr/bin/env python3
"""
Script para crear los notebooks del proyecto MongoDB Replicación
"""

import json
import os

def create_eda_notebook():
    """Crear el notebook de EDA y ETL"""
    
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# 📊 EDA y ETL - Proyecto MongoDB Replicación\n",
                    "\n",
                    "## 🎯 Objetivos\n",
                    "- Descargar dataset de Kaggle (Brazilian E-commerce)\n",
                    "- Realizar Exploratory Data Analysis (EDA)\n",
                    "- Procesar y limpiar datos (ETL)\n",
                    "- Cargar datos en MongoDB con replicación\n",
                    "- Verificar la replicación de datos\n",
                    "\n",
                    "---"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 1. 📦 Importar Librerías"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Librerías para análisis de datos\n",
                    "import pandas as pd\n",
                    "import numpy as np\n",
                    "import matplotlib.pyplot as plt\n",
                    "import seaborn as sns\n",
                    "from datetime import datetime, timedelta\n",
                    "from pymongo import MongoClient\n",
                    "import warnings\n",
                    "warnings.filterwarnings('ignore')\n",
                    "\n",
                    "# Configurar estilo de gráficos\n",
                    "plt.style.use('seaborn-v0_8')\n",
                    "sns.set_palette(\"husl\")\n",
                    "plt.rcParams['figure.figsize'] = (12, 8)\n",
                    "\n",
                    "print(\"✅ Librerías importadas correctamente\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 2. 📥 Descargar Dataset de Kaggle"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Descargar dataset de Brazilian E-commerce\n",
                    "import kagglehub\n",
                    "\n",
                    "print(\"📥 Descargando dataset de Kaggle...\")\n",
                    "path = kagglehub.dataset_download(\"olistbr/brazilian-ecommerce\")\n",
                    "print(f\"✅ Dataset descargado en: {path}\")\n",
                    "\n",
                    "# Listar archivos descargados\n",
                    "import os\n",
                    "files = os.listdir(path)\n",
                    "print(f\"📁 Archivos disponibles: {files}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 3. 📖 Cargar y Explorar Datos"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Cargar archivos CSV\n",
                    "print(\"📖 Cargando archivos CSV...\")\n",
                    "\n",
                    "# Archivo principal de órdenes\n",
                    "orders_df = pd.read_csv(os.path.join(path, 'olist_orders_dataset.csv'))\n",
                    "print(f\"📊 Órdenes: {orders_df.shape}\")\n",
                    "\n",
                    "# Archivo de items de órdenes\n",
                    "order_items_df = pd.read_csv(os.path.join(path, 'olist_order_items_dataset.csv'))\n",
                    "print(f\"📦 Items: {order_items_df.shape}\")\n",
                    "\n",
                    "# Archivo de productos\n",
                    "products_df = pd.read_csv(os.path.join(path, 'olist_products_dataset.csv'))\n",
                    "print(f\"🛍️ Productos: {products_df.shape}\")\n",
                    "\n",
                    "# Archivo de clientes\n",
                    "customers_df = pd.read_csv(os.path.join(path, 'olist_customers_dataset.csv'))\n",
                    "print(f\"👥 Clientes: {customers_df.shape}\")\n",
                    "\n",
                    "# Archivo de vendedores\n",
                    "sellers_df = pd.read_csv(os.path.join(path, 'olist_sellers_dataset.csv'))\n",
                    "print(f\"🏪 Vendedores: {sellers_df.shape}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 4. 🔍 Exploratory Data Analysis (EDA)"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Información general de los datasets\n",
                    "print(\"🔍 INFORMACIÓN GENERAL DE LOS DATASETS\")\n",
                    "print(\"=\" * 50)\n",
                    "\n",
                    "datasets = {\n",
                    "    'Órdenes': orders_df,\n",
                    "    'Items': order_items_df,\n",
                    "    'Productos': products_df,\n",
                    "    'Clientes': customers_df,\n",
                    "    'Vendedores': sellers_df\n",
                    "}\n",
                    "\n",
                    "for name, df in datasets.items():\n",
                    "    print(f\"\\n📊 {name}:\")\n",
                    "    print(f\"   Filas: {df.shape[0]:,}\")\n",
                    "    print(f\"   Columnas: {df.shape[1]}\")\n",
                    "    print(f\"   Memoria: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB\")\n",
                    "    print(f\"   Valores nulos: {df.isnull().sum().sum()}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 5. 🧹 ETL - Extracción, Transformación y Limpieza"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Crear dataset consolidado para MongoDB\n",
                    "print(\"🔄 Creando dataset consolidado...\")\n",
                    "\n",
                    "# Unir todos los datos\n",
                    "ventas_consolidado = (\n",
                    "    order_items_df\n",
                    "    .merge(orders_df, on='order_id', how='inner')\n",
                    "    .merge(products_df, on='product_id', how='left')\n",
                    "    .merge(customers_df, on='customer_id', how='left')\n",
                    "    .merge(sellers_df, on='seller_id', how='left')\n",
                    ")\n",
                    "\n",
                    "print(f\"📊 Dataset consolidado: {ventas_consolidado.shape}\")\n",
                    "print(f\"📋 Columnas: {list(ventas_consolidado.columns)}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Limpiar y transformar datos\n",
                    "print(\"🧹 Limpiando y transformando datos...\")\n",
                    "\n",
                    "# Seleccionar columnas relevantes y renombrar\n",
                    "ventas_limpio = ventas_consolidado[[\n",
                    "    'order_id', 'order_item_id', 'product_id', 'seller_id', 'customer_id',\n",
                    "    'order_purchase_timestamp', 'order_status', 'price', 'freight_value',\n",
                    "    'product_category_name', 'product_name_lenght', 'product_description_lenght',\n",
                    "    'product_photos_qty', 'product_weight_g', 'product_length_cm',\n",
                    "    'product_height_cm', 'product_width_cm', 'customer_city', 'customer_state',\n",
                    "    'seller_city', 'seller_state'\n",
                    "]].copy()\n",
                    "\n",
                    "# Renombrar columnas para mejor legibilidad\n",
                    "ventas_limpio.columns = [\n",
                    "    'id_orden', 'id_item', 'id_producto', 'id_vendedor', 'id_cliente',\n",
                    "    'fecha_compra', 'estado_orden', 'precio', 'costo_envio',\n",
                    "    'categoria_producto', 'longitud_nombre', 'longitud_descripcion',\n",
                    "    'cantidad_fotos', 'peso_gramos', 'longitud_cm',\n",
                    "    'altura_cm', 'ancho_cm', 'ciudad_cliente', 'estado_cliente',\n",
                    "    'ciudad_vendedor', 'estado_vendedor'\n",
                    "]\n",
                    "\n",
                    "# Convertir fecha_compra a datetime\n",
                    "ventas_limpio['fecha_compra'] = pd.to_datetime(ventas_limpio['fecha_compra'], errors='coerce')\n",
                    "\n",
                    "# Agregar campos calculados\n",
                    "ventas_limpio['precio_total'] = ventas_limpio['precio'] + ventas_limpio['costo_envio']\n",
                    "ventas_limpio['volumen_cm3'] = ventas_limpio['longitud_cm'] * ventas_limpio['altura_cm'] * ventas_limpio['ancho_cm']\n",
                    "\n",
                    "# Agregar campo de stock simulado\n",
                    "ventas_limpio['cantidad_stock'] = np.random.randint(0, 100, len(ventas_limpio))\n",
                    "\n",
                    "# Limpiar valores nulos\n",
                    "ventas_limpio = ventas_limpio.dropna(subset=['fecha_compra', 'precio'])\n",
                    "\n",
                    "print(f\"✅ Dataset limpio: {ventas_limpio.shape}\")\n",
                    "print(f\"📋 Columnas finales: {list(ventas_limpio.columns)}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 6. 🗄️ Conectar a MongoDB y Cargar Datos"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Conectar a MongoDB\n",
                    "print(\"🔌 Conectando a MongoDB...\")\n",
                    "\n",
                    "try:\n",
                    "    # Conectar al nodo primario directamente\n",
                    "    client = MongoClient('mongodb://localhost:27020/', \n",
                    "                        directConnection=True,\n",
                    "                        serverSelectionTimeoutMS=5000)\n",
                    "    \n",
                    "    # Verificar conexión\n",
                    "    client.admin.command('ping')\n",
                    "    print(\"✅ Conexión exitosa a MongoDB\")\n",
                    "    \n",
                    "    # Seleccionar base de datos\n",
                    "    db = client['ecommerce_brazil']\n",
                    "    collection = db['ventas']\n",
                    "    \n",
                    "    print(f\"📊 Base de datos: {db.name}\")\n",
                    "    print(f\"📋 Colección: {collection.name}\")\n",
                    "    \n",
                    "except Exception as e:\n",
                    "    print(f\"❌ Error de conexión: {e}\")\n",
                    "    print(\"💡 Asegúrate de que el cluster MongoDB esté ejecutándose\")\n",
                    "    print(\"   docker-compose -f docker/docker-compose.yml up -d\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Limpiar colección existente\n",
                    "print(\"🧹 Limpiando colección existente...\")\n",
                    "collection.delete_many({})\n",
                    "print(f\"✅ Colección limpiada\")\n",
                    "\n",
                    "# Convertir DataFrame a documentos MongoDB\n",
                    "print(\"🔄 Convirtiendo datos a formato MongoDB...\")\n",
                    "ventas_limpio['fecha_compra'] = ventas_limpio['fecha_compra'].astype(str)\n",
                    "documents = ventas_limpio.to_dict('records')\n",
                    "\n",
                    "print(f\"📄 Documentos a insertar: {len(documents):,}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Insertar datos en MongoDB\n",
                    "print(\"📥 Insertando datos en MongoDB...\")\n",
                    "\n",
                    "from tqdm import tqdm\n",
                    "\n",
                    "# Insertar en lotes para mejor rendimiento\n",
                    "batch_size = 1000\n",
                    "total_inserted = 0\n",
                    "\n",
                    "for i in tqdm(range(0, len(documents), batch_size), desc=\"Insertando documentos\"):\n",
                    "    batch = documents[i:i + batch_size]\n",
                    "    result = collection.insert_many(batch)\n",
                    "    total_inserted += len(result.inserted_ids)\n",
                    "\n",
                    "print(f\"✅ {total_inserted:,} documentos insertados exitosamente\")\n",
                    "print(f\"📊 Total en colección: {collection.count_documents({}):,}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 7. 🔄 Verificar Replicación"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Verificar replicación en nodos secundarios\n",
                    "print(\"🔄 Verificando replicación en nodos secundarios...\")\n",
                    "\n",
                    "try:\n",
                    "    # Verificar datos en primario\n",
                    "    primary_count = collection.count_documents({})\n",
                    "    print(f\"✅ Primario (puerto 27020): {primary_count:,} documentos\")\n",
                    "    \n",
                    "    # Verificar replicación usando comandos Docker\n",
                    "    import subprocess\n",
                    "    \n",
                    "    # Verificar secundario 1\n",
                    "    result1 = subprocess.run([\n",
                    "        'docker', 'exec', '-i', 'mongo-secondary1', 'mongosh', \n",
                    "        '--eval', 'db.ecommerce_brazil.ventas.countDocuments()'\n",
                    "    ], capture_output=True, text=True)\n",
                    "    \n",
                    "    if result1.returncode == 0:\n",
                    "        count1 = int(result1.stdout.strip().split('\\n')[-1])\n",
                    "        print(f\"✅ Secundario 1 (puerto 27021): {count1:,} documentos\")\n",
                    "    else:\n",
                    "        print(f\"⚠️ Error verificando secundario 1: {result1.stderr}\")\n",
                    "        count1 = 0\n",
                    "    \n",
                    "    # Verificar secundario 2\n",
                    "    result2 = subprocess.run([\n",
                    "        'docker', 'exec', '-i', 'mongo-secondary2', 'mongosh', \n",
                    "        '--eval', 'db.ecommerce_brazil.ventas.countDocuments()'\n",
                    "    ], capture_output=True, text=True)\n",
                    "    \n",
                    "    if result2.returncode == 0:\n",
                    "        count2 = int(result2.stdout.strip().split('\\n')[-1])\n",
                    "        print(f\"✅ Secundario 2 (puerto 27022): {count2:,} documentos\")\n",
                    "    else:\n",
                    "        print(f\"⚠️ Error verificando secundario 2: {result2.stderr}\")\n",
                    "        count2 = 0\n",
                    "    \n",
                    "    # Verificar consistencia\n",
                    "    if count1 == count2 == primary_count:\n",
                    "        print(\"🎉 ¡Replicación exitosa! Todos los nodos tienen la misma cantidad de datos\")\n",
                    "    else:\n",
                    "        print(\"⚠️ Advertencia: Los nodos no tienen la misma cantidad de datos\")\n",
                    "        print(f\"   Primario: {primary_count}, Secundario 1: {count1}, Secundario 2: {count2}\")\n",
                    "        \n",
                    "except Exception as e:\n",
                    "    print(f\"❌ Error verificando replicación: {e}\")\n",
                    "    print(\"💡 Puedes verificar manualmente con:\")\n",
                    "    print(\"   docker exec -it mongo-primary mongosh --eval \\\"db.ecommerce_brazil.ventas.countDocuments()\\\"\")\n",
                    "    print(\"   docker exec -it mongo-secondary1 mongosh --eval \\\"db.ecommerce_brazil.ventas.countDocuments()\\\"\")\n",
                    "    print(\"   docker exec -it mongo-secondary2 mongosh --eval \\\"db.ecommerce_brazil.ventas.countDocuments()\\\"\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Cerrar conexión\n",
                    "client.close()\n",
                    "print(\"🔌 Conexión cerrada\")"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    # Guardar notebook
    with open('notebooks/EDA_ETL_MongoDB.ipynb', 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)
    
    print("✅ Notebook EDA_ETL_MongoDB.ipynb creado")

def create_crud_notebook():
    """Crear el notebook de consultas CRUD"""
    
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# 🔍 Consultas CRUD - Proyecto MongoDB Replicación\n",
                    "\n",
                    "## 🎯 Objetivos\n",
                    "- Realizar 15 consultas CRUD complejas\n",
                    "- Probar operaciones de lectura y escritura\n",
                    "- Verificar consistencia en replicación\n",
                    "- Analizar rendimiento de consultas\n",
                    "\n",
                    "---"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 1. 📦 Importar Librerías y Conectar"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "from pymongo import MongoClient\n",
                    "import pandas as pd\n",
                    "import matplotlib.pyplot as plt\n",
                    "import seaborn as sns\n",
                    "from datetime import datetime, timedelta\n",
                    "import warnings\n",
                    "warnings.filterwarnings('ignore')\n",
                    "\n",
                    "# Conectar a MongoDB\n",
                    "client = MongoClient('mongodb://localhost:27020/', directConnection=True)\n",
                    "db = client['ecommerce_brazil']\n",
                    "collection = db['ventas']\n",
                    "\n",
                    "print(\"✅ Conectado a MongoDB\")\n",
                    "print(f\"📊 Documentos en colección: {collection.count_documents({}):,}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 2. 🔍 Consultas CRUD"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Consulta 1: Total de ventas por estado\n",
                    "print(\"🔍 Consulta 1: Total de ventas por estado\")\n",
                    "pipeline = [\n",
                    "    {\"$group\": {\"_id\": \"$estado_cliente\", \"total_ventas\": {\"$sum\": \"$precio_total\"}}},\n",
                    "    {\"$sort\": {\"total_ventas\": -1}}\n",
                    "]\n",
                    "\n",
                    "result = list(collection.aggregate(pipeline))\n",
                    "df_result = pd.DataFrame(result)\n",
                    "print(df_result.head(10))"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    # Guardar notebook
    with open('notebooks/Consultas_CRUD.ipynb', 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)
    
    print("✅ Notebook Consultas_CRUD.ipynb creado")

if __name__ == "__main__":
    print("🚀 Creando notebooks del proyecto...")
    create_eda_notebook()
    create_crud_notebook()
    print("🎉 ¡Notebooks creados exitosamente!") 