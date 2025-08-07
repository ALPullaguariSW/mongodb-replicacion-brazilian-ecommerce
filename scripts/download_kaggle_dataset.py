#!/usr/bin/env python3
"""
Script para descargar el dataset de Brazilian E-Commerce de Kaggle
Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
"""

import os
import zipfile
import requests
from pathlib import Path
import pandas as pd

def download_kaggle_dataset():
    """
    Descarga el dataset de Brazilian E-Commerce de Kaggle
    """
    print("🚀 Iniciando descarga del dataset de Brazilian E-Commerce...")
    
    # URLs de los archivos CSV del dataset
    dataset_urls = {
        'olist_customers_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_customers_dataset.csv',
        'olist_geolocation_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_geolocation_dataset.csv',
        'olist_order_items_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_items_dataset.csv',
        'olist_order_payments_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_payments_dataset.csv',
        'olist_order_reviews_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_reviews_dataset.csv',
        'olist_orders_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_orders_dataset.csv',
        'olist_products_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_products_dataset.csv',
        'olist_sellers_dataset.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_sellers_dataset.csv',
        'product_category_name_translation.csv': 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/product_category_name_translation.csv'
    }
    
    # Crear directorio de datos si no existe
    data_dir = Path('data/raw')
    data_dir.mkdir(parents=True, exist_ok=True)
    
    downloaded_files = []
    
    for filename, url in dataset_urls.items():
        file_path = data_dir / filename
        
        if file_path.exists():
            print(f"✅ {filename} ya existe, saltando...")
            downloaded_files.append(filename)
            continue
            
        try:
            print(f"📥 Descargando {filename}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"✅ {filename} descargado exitosamente")
            downloaded_files.append(filename)
            
        except Exception as e:
            print(f"❌ Error descargando {filename}: {e}")
    
    print(f"\n📊 Archivos descargados: {len(downloaded_files)}")
    print("📁 Los archivos se guardaron en: data/raw/")
    
    return downloaded_files

def show_dataset_info():
    """
    Muestra información básica sobre los archivos descargados
    """
    print("\n" + "="*60)
    print("📋 INFORMACIÓN DEL DATASET")
    print("="*60)
    
    data_dir = Path('data/raw')
    
    if not data_dir.exists():
        print("❌ No se encontró el directorio de datos")
        return
    
    csv_files = list(data_dir.glob('*.csv'))
    
    if not csv_files:
        print("❌ No se encontraron archivos CSV")
        return
    
    print(f"📁 Total de archivos CSV: {len(csv_files)}")
    print("\n📊 Resumen de archivos:")
    
    for file_path in csv_files:
        try:
            # Leer las primeras filas para obtener información
            df = pd.read_csv(file_path, nrows=5)
            file_size = file_path.stat().st_size / (1024 * 1024)  # MB
            
            print(f"  • {file_path.name}")
            print(f"    - Tamaño: {file_size:.2f} MB")
            print(f"    - Columnas: {len(df.columns)}")
            print(f"    - Muestra de columnas: {list(df.columns[:5])}")
            print()
            
        except Exception as e:
            print(f"  • {file_path.name} - Error leyendo: {e}")

if __name__ == "__main__":
    print("🎯 SCRIPT DE DESCARGA DE DATASET KAGGLE")
    print("="*50)
    
    # Descargar dataset
    downloaded_files = download_kaggle_dataset()
    
    # Mostrar información
    show_dataset_info()
    
    print("\n✅ Proceso completado!")
    print("📝 Próximos pasos:")
    print("   1. Revisar los archivos descargados")
    print("   2. Ejecutar el notebook de EDA")
    print("   3. Realizar el proceso ETL")
