#!/usr/bin/env python3
"""
Script para crear el notebook de EDA y ETL con integración de kagglehub
"""

import json
import os

def create_eda_notebook():
    """Crear el notebook completo de EDA y ETL"""
    
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# EDA y ETL MongoDB - Replicación Primario-Secundario\n",
                    "## Dataset: Brazilian E-commerce (Kaggle)\n",
                    "\n",
                    "Este notebook realiza:\n",
                    "1. **Descarga automática** del dataset desde Kaggle usando kagglehub\n",
                    "2. **Análisis Exploratorio de Datos (EDA)** de los archivos CSV\n",
                    "3. **Extracción, Transformación y Carga (ETL)**\n",
                    "4. **Carga a MongoDB** con verificación de replicación"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Importar librerías necesarias\n",
                    "import pandas as pd\n",
                    "import numpy as np\n",
                    "import matplotlib.pyplot as plt\n",
                    "import seaborn as sns\n",
                    "from datetime import datetime, timedelta\n",
                    "from pymongo import MongoClient\n",
                    "import warnings\n",
                    "import os\n",
                    "import kagglehub\n",
                    "from pathlib import Path\n",
                    "\n",
                    "warnings.filterwarnings('ignore')\n",
                    "\n",
                    "# Configurar estilo de gráficos\n",
                    "plt.style.use('seaborn-v0_8')\n",
                    "sns.set_palette(\"husl\")\n",
                    "\n",
                    "print(\"✅ Librerías importadas correctamente\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 1. Descarga del Dataset desde Kaggle"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Descargar dataset usando kagglehub\n",
                    "print(\"📥 Descargando dataset de Brazilian E-commerce...\")\n",
                    "\n",
                    "try:\n",
                    "    # Descargar el dataset\n",
                    "    path = kagglehub.dataset_download(\"olistbr/brazilian-ecommerce\")\n",
                    "    print(f\"✅ Dataset descargado en: {path}\")\n",
                    "    \n",
                    "    # Listar archivos descargados\n",
                    "    files = list(Path(path).glob(\"*.csv\"))\n",
                    "    print(f\"\\n📁 Archivos CSV encontrados ({len(files)}):\")\n",
                    "    for file in files:\n",
                    "        print(f\"  - {file.name}\")\n",
                    "        \n",
                    "except Exception as e:\n",
                    "    print(f\"❌ Error al descargar: {e}\")\n",
                    "    print(\"💡 Asegúrate de tener kagglehub instalado: pip install kagglehub\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 2. Carga y Exploración de los Datos"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Cargar todos los archivos CSV\n",
                    "print(\"📊 Cargando archivos CSV...\")\n",
                    "\n",
                    "dataframes = {}\n",
                    "\n",
                    "for file in files:\n",
                    "    df_name = file.stem  # Nombre del archivo sin extensión\n",
                    "    print(f\"\\n📖 Cargando {file.name}...\")\n",
                    "    \n",
                    "    try:\n",
                    "        df = pd.read_csv(file)\n",
                    "        dataframes[df_name] = df\n",
                    "        print(f\"  ✅ Filas: {len(df)}, Columnas: {len(df.columns)}\")\n",
                    "        print(f\"  📋 Columnas: {list(df.columns)}\")\n",
                    "        \n",
                    "    except Exception as e:\n",
                    "        print(f\"  ❌ Error al cargar {file.name}: {e}\")\n",
                    "\n",
                    "print(f\"\\n🎉 Total de datasets cargados: {len(dataframes)}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 3. Análisis Exploratorio de Datos (EDA)"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Mostrar información básica de cada dataset\n",
                    "print(\"🔍 ANÁLISIS EXPLORATORIO DE DATOS\")\n",
                    "print(\"=\" * 50)\n",
                    "\n",
                    "for name, df in dataframes.items():\n",
                    "    print(f\"\\n📊 DATASET: {name.upper()}\")\n",
                    "    print(f\"Dimensiones: {df.shape}\")\n",
                    "    print(f\"\\nPrimeras 3 filas:\")\n",
                    "    display(df.head(3))\n",
                    "    \n",
                    "    print(f\"\\nInformación del dataset:\")\n",
                    "    print(df.info())\n",
                    "    \n",
                    "    print(f\"\\nValores nulos:\")\n",
                    "    null_counts = df.isnull().sum()\n",
                    "    if null_counts.sum() > 0:\n",
                    "        print(null_counts[null_counts > 0])\n",
                    "    else:\n",
                    "        print(\"✅ No hay valores nulos\")\n",
                    "    \n",
                    "    print(f\"\\nEstadísticas descriptivas:\")\n",
                    "    display(df.describe())\n",
                    "    \n",
                    "    print(\"-\" * 50)"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 4. Limpieza y Transformación de Datos (ETL)"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# ETL: Combinar datasets para crear un dataset unificado de ventas\n",
                    "print(\"🔄 PROCESO ETL - COMBINANDO DATASETS\")\n",
                    "print(\"=\" * 50)\n",
                    "\n",
                    "# Obtener los datasets principales\n",
                    "orders_df = dataframes.get('olist_orders_dataset', pd.DataFrame())\n",
                    "items_df = dataframes.get('olist_order_items_dataset', pd.DataFrame())\n",
                    "products_df = dataframes.get('olist_products_dataset', pd.DataFrame())\n",
                    "customers_df = dataframes.get('olist_customers_dataset', pd.DataFrame())\n",
                    "sellers_df = dataframes.get('olist_sellers_dataset', pd.DataFrame())\n",
                    "\n",
                    "print(f\"📦 Orders: {orders_df.shape}\")\n",
                    "print(f\"📦 Items: {items_df.shape}\")\n",
                    "print(f\"📦 Products: {products_df.shape}\")\n",
                    "print(f\"📦 Customers: {customers_df.shape}\")\n",
                    "print(f\"📦 Sellers: {sellers_df.shape}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Limpiar y transformar fechas\n",
                    "print(\"\\n🕒 Transformando fechas...\")\n",
                    "\n",
                    "if not orders_df.empty:\n",
                    "    # Convertir columnas de fecha\n",
                    "    date_columns = ['order_purchase_date', 'order_approved_at', 'order_delivered_carrier_date', \n",
                    "                   'order_delivered_customer_date', 'order_estimated_delivery_date']\n",
                    "    \n",
                    "    for col in date_columns:\n",
                    "        if col in orders_df.columns:\n",
                    "            orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')\n",
                    "    \n",
                    "    print(\"✅ Fechas transformadas\")\n",
                    "    \n",
                    "    # Mostrar rango de fechas\n",
                    "    if 'order_purchase_date' in orders_df.columns:\n",
                    "        print(f\"📅 Rango de fechas de compra: {orders_df['order_purchase_date'].min()} a {orders_df['order_purchase_date'].max()}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Combinar datasets\n",
                    "print(\"\\n🔗 Combinando datasets...\")\n",
                    "\n",
                    "try:\n",
                    "    # Merge 1: Orders + Items\n",
                    "    if not orders_df.empty and not items_df.empty:\n",
                    "        ventas_df = orders_df.merge(items_df, on='order_id', how='inner')\n",
                    "        print(f\"✅ Orders + Items: {ventas_df.shape}\")\n",
                    "    \n",
                    "    # Merge 2: + Products\n",
                    "    if not products_df.empty:\n",
                    "        ventas_df = ventas_df.merge(products_df, on='product_id', how='left')\n",
                    "        print(f\"✅ + Products: {ventas_df.shape}\")\n",
                    "    \n",
                    "    # Merge 3: + Customers\n",
                    "    if not customers_df.empty:\n",
                    "        ventas_df = ventas_df.merge(customers_df, on='customer_id', how='left')\n",
                    "        print(f\"✅ + Customers: {ventas_df.shape}\")\n",
                    "    \n",
                    "    # Merge 4: + Sellers\n",
                    "    if not sellers_df.empty:\n",
                    "        ventas_df = ventas_df.merge(sellers_df, on='seller_id', how='left')\n",
                    "        print(f\"✅ + Sellers: {ventas_df.shape}\")\n",
                    "    \n",
                    "    print(f\"\\n🎉 Dataset combinado final: {ventas_df.shape}\")\n",
                    "    \n",
                    "except Exception as e:\n",
                    "    print(f\"❌ Error al combinar: {e}\")\n",
                    "    # Crear dataset de ejemplo si falla la combinación\n",
                    "    ventas_df = pd.DataFrame()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Limpiar y preparar el dataset final\n",
                    "print(\"\\n🧹 Limpiando dataset final...\")\n",
                    "\n",
                    "if not ventas_df.empty:\n",
                    "    # Seleccionar columnas relevantes y renombrar\n",
                    "    columnas_finales = {\n",
                    "        'order_id': 'pedido_id',\n",
                    "        'order_purchase_date': 'fecha_compra',\n",
                    "        'order_status': 'estado_pedido',\n",
                    "        'product_id': 'producto_id',\n",
                    "        'product_name_lenght': 'longitud_nombre_producto',\n",
                    "        'product_description_lenght': 'longitud_descripcion_producto',\n",
                    "        'product_photos_qty': 'cantidad_fotos_producto',\n",
                    "        'product_weight_g': 'peso_producto_g',\n",
                    "        'product_length_cm': 'longitud_producto_cm',\n",
                    "        'product_height_cm': 'altura_producto_cm',\n",
                    "        'product_width_cm': 'ancho_producto_cm',\n",
                    "        'price': 'precio',\n",
                    "        'freight_value': 'valor_flete',\n",
                    "        'customer_id': 'cliente_id',\n",
                    "        'customer_city': 'ciudad_cliente',\n",
                    "        'customer_state': 'estado_cliente',\n",
                    "        'seller_id': 'vendedor_id',\n",
                    "        'seller_city': 'ciudad_vendedor',\n",
                    "        'seller_state': 'estado_vendedor'\n",
                    "    }\n",
                    "    \n",
                    "    # Filtrar columnas que existen\n",
                    "    columnas_existentes = {k: v for k, v in columnas_finales.items() if k in ventas_df.columns}\n",
                    "    ventas_limpio = ventas_df[list(columnas_existentes.keys())].copy()\n",
                    "    ventas_limpio.rename(columns=columnas_existentes, inplace=True)\n",
                    "    \n",
                    "    # Agregar campos calculados\n",
                    "    if 'precio' in ventas_limpio.columns and 'valor_flete' in ventas_limpio.columns:\n",
                    "        ventas_limpio['precio_total'] = ventas_limpio['precio'] + ventas_limpio['valor_flete']\n",
                    "    \n",
                    "    # Agregar campo de stock simulado\n",
                    "    ventas_limpio['cantidad_stock'] = np.random.randint(0, 100, len(ventas_limpio))\n",
                    "    \n",
                    "    # Limpiar valores nulos\n",
                    "    ventas_limpio.dropna(subset=['fecha_compra', 'precio'], inplace=True)\n",
                    "    \n",
                    "    print(f\"✅ Dataset limpio: {ventas_limpio.shape}\")\n",
                    "    print(f\"📋 Columnas finales: {list(ventas_limpio.columns)}\")\n",
                    "    \n",
                    "else:\n",
                    "    print(\"⚠️ No se pudo crear el dataset combinado, usando datos de ejemplo\")\n",
                    "    # Crear dataset de ejemplo\n",
                    "    ventas_limpio = pd.DataFrame({\n",
                    "        'pedido_id': range(1, 1001),\n",
                    "        'fecha_compra': pd.date_range('2023-01-01', periods=1000, freq='D'),\n",
                    "        'producto_id': np.random.randint(1, 101, 1000),\n",
                    "        'precio': np.random.uniform(10, 500, 1000),\n",
                    "        'cliente_id': np.random.randint(1, 201, 1000),\n",
                    "        'ciudad_cliente': np.random.choice(['São Paulo', 'Rio de Janeiro', 'Brasília', 'Salvador', 'Fortaleza'], 1000),\n",
                    "        'cantidad_stock': np.random.randint(0, 100, 1000)\n",
                    "    })"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 5. Visualizaciones del EDA"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Visualizaciones del EDA\n",
                    "print(\"📊 CREANDO VISUALIZACIONES\")\n",
                    "print(\"=\" * 50)\n",
                    "\n",
                    "# Configurar subplots\n",
                    "fig, axes = plt.subplots(2, 2, figsize=(15, 12))\n",
                    "fig.suptitle('Análisis Exploratorio de Datos - Brazilian E-commerce', fontsize=16, fontweight='bold')\n",
                    "\n",
                    "# 1. Distribución de precios\n",
                    "if 'precio' in ventas_limpio.columns:\n",
                    "    axes[0, 0].hist(ventas_limpio['precio'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')\n",
                    "    axes[0, 0].set_title('Distribución de Precios')\n",
                    "    axes[0, 0].set_xlabel('Precio (R$)')\n",
                    "    axes[0, 0].set_ylabel('Frecuencia')\n",
                    "    axes[0, 0].grid(True, alpha=0.3)\n",
                    "\n",
                    "# 2. Ventas por mes\n",
                    "if 'fecha_compra' in ventas_limpio.columns:\n",
                    "    ventas_por_mes = ventas_limpio.groupby(ventas_limpio['fecha_compra'].dt.to_period('M')).size()\n",
                    "    axes[0, 1].plot(range(len(ventas_por_mes)), ventas_por_mes.values, marker='o', linewidth=2, markersize=6)\n",
                    "    axes[0, 1].set_title('Ventas por Mes')\n",
                    "    axes[0, 1].set_xlabel('Mes')\n",
                    "    axes[0, 1].set_ylabel('Número de Ventas')\n",
                    "    axes[0, 1].grid(True, alpha=0.3)\n",
                    "\n",
                    "# 3. Top ciudades por ventas\n",
                    "if 'ciudad_cliente' in ventas_limpio.columns:\n",
                    "    top_ciudades = ventas_limpio['ciudad_cliente'].value_counts().head(10)\n",
                    "    axes[1, 0].barh(range(len(top_ciudades)), top_ciudades.values, color='lightcoral')\n",
                    "    axes[1, 0].set_yticks(range(len(top_ciudades)))\n",
                    "    axes[1, 0].set_yticklabels(top_ciudades.index)\n",
                    "    axes[1, 0].set_title('Top 10 Ciudades por Ventas')\n",
                    "    axes[1, 0].set_xlabel('Número de Ventas')\n",
                    "    axes[1, 0].grid(True, alpha=0.3)\n",
                    "\n",
                    "# 4. Distribución de stock\n",
                    "if 'cantidad_stock' in ventas_limpio.columns:\n",
                    "    axes[1, 1].hist(ventas_limpio['cantidad_stock'], bins=20, alpha=0.7, color='lightgreen', edgecolor='black')\n",
                    "    axes[1, 1].set_title('Distribución de Stock')\n",
                    "    axes[1, 1].set_xlabel('Cantidad en Stock')\n",
                    "    axes[1, 1].set_ylabel('Frecuencia')\n",
                    "    axes[1, 1].grid(True, alpha=0.3)\n",
                    "\n",
                    "plt.tight_layout()\n",
                    "plt.show()\n",
                    "\n",
                    "print(\"✅ Visualizaciones creadas\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 6. Conexión y Carga a MongoDB"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Configuración de MongoDB\n",
                    "MONGO_URI = \"mongodb://admin:password123@localhost:27017/\"\n",
                    "DB_NAME = \"ventas_tienda_db\"\n",
                    "COLLECTION_NAME = \"ventas\"\n",
                    "\n",
                    "print(\"🔌 Conectando a MongoDB...\")\n",
                    "\n",
                    "try:\n",
                    "    client = MongoClient(MONGO_URI)\n",
                    "    client.admin.command('ping')\n",
                    "    print(\"✅ Conexión exitosa a MongoDB\")\n",
                    "    \n",
                    "    db = client[DB_NAME]\n",
                    "    collection = db[COLLECTION_NAME]\n",
                    "    \n",
                    "    # Limpiar colección existente\n",
                    "    collection.delete_many({})\n",
                    "    print(\"🧹 Colección limpiada\")\n",
                    "    \n",
                    "except Exception as e:\n",
                    "    print(f\"❌ Error de conexión: {e}\")\n",
                    "    print(\"💡 Asegúrate de que MongoDB esté ejecutándose con: docker-compose -f docker/docker-compose.yml up -d\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Cargar datos a MongoDB\n",
                    "print(\"\\n📤 Cargando datos a MongoDB...\")\n",
                    "\n",
                    "try:\n",
                    "    # Convertir DataFrame a documentos\n",
                    "    records = ventas_limpio.to_dict('records')\n",
                    "    \n",
                    "    # Procesar fechas\n",
                    "    for record in records:\n",
                    "        if isinstance(record.get('fecha_compra'), str):\n",
                    "            record['fecha_compra'] = pd.to_datetime(record['fecha_compra'])\n",
                    "    \n",
                    "    # Insertar en lotes para mejor rendimiento\n",
                    "    batch_size = 1000\n",
                    "    total_inserted = 0\n",
                    "    \n",
                    "    for i in range(0, len(records), batch_size):\n",
                    "        batch = records[i:i + batch_size]\n",
                    "        result = collection.insert_many(batch)\n",
                    "        total_inserted += len(result.inserted_ids)\n",
                    "        print(f\"  📦 Lote {i//batch_size + 1}: {len(result.inserted_ids)} registros\")\n",
                    "    \n",
                    "    print(f\"\\n🎉 Total de registros insertados: {total_inserted}\")\n",
                    "    \n",
                    "    # Verificar inserción\n",
                    "    count = collection.count_documents({})\n",
                    "    print(f\"📊 Documentos en la colección: {count}\")\n",
                    "    \n",
                    "except Exception as e:\n",
                    "    print(f\"❌ Error al cargar datos: {e}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 7. Verificación de Replicación"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Verificar replicación entre nodos\n",
                    "print(\"🔄 VERIFICANDO REPLICACIÓN\")\n",
                    "print(\"=\" * 50)\n",
                    "\n",
                    "try:\n",
                    "    # Insertar documento de prueba en el primario\n",
                    "    test_doc = {\n",
                    "        'pedido_id': 'TEST-001',\n",
                    "        'fecha_compra': datetime.now(),\n",
                    "        'producto_id': 'PROD-TEST',\n",
                    "        'precio': 999.99,\n",
                    "        'cliente_id': 99999,\n",
                    "        'ciudad_cliente': 'Ciudad de Prueba',\n",
                    "        'cantidad_stock': 50,\n",
                    "        'test_replicacion': True\n",
                    "    }\n",
                    "    \n",
                    "    result = collection.insert_one(test_doc)\n",
                    "    print(f\"✅ Documento de prueba insertado: {result.inserted_id}\")\n",
                    "    \n",
                    "    # Esperar a que se replique\n",
                    "    import time\n",
                    "    print(\"⏳ Esperando replicación...\")\n",
                    "    time.sleep(3)\n",
                    "    \n",
                    "    # Verificar en nodos secundarios\n",
                    "    secondary_ports = [27018, 27019]\n",
                    "    \n",
                    "    for port in secondary_ports:\n",
                    "        try:\n",
                    "            secondary_uri = f\"mongodb://admin:password123@localhost:{port}/\"\n",
                    "            secondary_client = MongoClient(secondary_uri)\n",
                    "            secondary_db = secondary_client[DB_NAME]\n",
                    "            secondary_collection = secondary_db[COLLECTION_NAME]\n",
                    "            \n",
                    "            # Buscar documento de prueba\n",
                    "            doc = secondary_collection.find_one({'pedido_id': 'TEST-001'})\n",
                    "            \n",
                    "            if doc:\n",
                    "                print(f\"✅ Replicación exitosa en puerto {port}: {doc['producto_id']} - ${doc['precio']}\")\n",
                    "            else:\n",
                    "                print(f\"❌ Replicación falló en puerto {port}\")\n",
                    "                \n",
                    "        except Exception as e:\n",
                    "            print(f\"⚠️ No se pudo verificar puerto {port}: {e}\")\n",
                    "    \n",
                    "    # Limpiar documento de prueba\n",
                    "    collection.delete_one({'pedido_id': 'TEST-001'})\n",
                    "    print(\"🧹 Documento de prueba eliminado\")\n",
                    "    \n",
                    "except Exception as e:\n",
                    "    print(f\"❌ Error en verificación: {e}\")\n",
                    "    print(\"💡 Verifica que el replica set esté configurado correctamente\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 8. Resumen del EDA y ETL"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Resumen final\n",
                    "print(\"📋 RESUMEN DEL PROCESO EDA Y ETL\")\n",
                    "print(\"=\" * 50)\n",
                    "\n",
                    "print(f\"\\n📊 DATASET ORIGINAL:\")\n",
                    "for name, df in dataframes.items():\n",
                    "    print(f\"  - {name}: {df.shape}\")\n",
                    "\n",
                    "print(f\"\\n🔄 DATASET PROCESADO:\")\n",
                    "print(f\"  - Dimensiones: {ventas_limpio.shape}\")\n",
                    "print(f\"  - Columnas: {list(ventas_limpio.columns)}\")\n",
                    "\n",
                    "print(f\"\\n📈 ESTADÍSTICAS CLAVE:\")\n",
                    "if 'precio' in ventas_limpio.columns:\n",
                    "    print(f\"  - Precio promedio: R$ {ventas_limpio['precio'].mean():.2f}\")\n",
                    "    print(f\"  - Precio máximo: R$ {ventas_limpio['precio'].max():.2f}\")\n",
                    "    print(f\"  - Precio mínimo: R$ {ventas_limpio['precio'].min():.2f}\")\n",
                    "\n",
                    "if 'fecha_compra' in ventas_limpio.columns:\n",
                    "    print(f\"  - Período: {ventas_limpio['fecha_compra'].min().date()} a {ventas_limpio['fecha_compra'].max().date()}\")\n",
                    "\n",
                    "if 'ciudad_cliente' in ventas_limpio.columns:\n",
                    "    print(f\"  - Ciudades únicas: {ventas_limpio['ciudad_cliente'].nunique()}\")\n",
                    "\n",
                    "print(f\"\\n🗄️ MONGODB:\")\n",
                    "try:\n",
                    "    count = collection.count_documents({})\n",
                    "    print(f\"  - Documentos cargados: {count}\")\n",
                    "    print(f\"  - Base de datos: {DB_NAME}\")\n",
                    "    print(f\"  - Colección: {COLLECTION_NAME}\")\n",
                    "except:\n",
                    "    print(\"  - No disponible\")\n",
                    "\n",
                    "print(f\"\\n🎉 ¡PROCESO COMPLETADO EXITOSAMENTE!\")\n",
                    "print(f\"💡 Ahora puedes ejecutar el notebook de Consultas CRUD para probar las operaciones\")"
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
    
    # Guardar el notebook
    notebook_path = "notebooks/EDA_ETL_MongoDB.ipynb"
    os.makedirs(os.path.dirname(notebook_path), exist_ok=True)
    
    with open(notebook_path, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Notebook creado exitosamente: {notebook_path}")
    print("📝 El notebook incluye:")
    print("  - Descarga automática con kagglehub")
    print("  - Análisis exploratorio completo")
    print("  - Proceso ETL con combinación de datasets")
    print("  - Visualizaciones")
    print("  - Carga a MongoDB")
    print("  - Verificación de replicación")

if __name__ == "__main__":
    create_eda_notebook() 