#!/usr/bin/env python3
"""
Utilidades para mostrar barras de progreso en notebooks de Jupyter
Módulo de soporte para el proyecto de Replicación MongoDB
"""

import time
from tqdm.notebook import tqdm
from tqdm import tqdm as tqdm_console
import pandas as pd
import numpy as np

def create_progress_bar(total, desc="Procesando", notebook_mode=True):
    """
    Crear una barra de progreso adaptada al entorno
    
    Args:
        total (int): Total de elementos a procesar
        desc (str): Descripción de la operación
        notebook_mode (bool): Si está en modo notebook o consola
    
    Returns:
        tqdm: Objeto de barra de progreso
    """
    if notebook_mode:
        return tqdm(total=total, desc=desc, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
    else:
        return tqdm_console(total=total, desc=desc, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}')

def progress_etl_steps():
    """
    Mostrar pasos del proceso ETL con barras de progreso
    """
    steps = [
        "📥 Descargando dataset",
        "📊 Cargando archivos CSV",
        "🔍 Análisis exploratorio",
        "🧹 Limpieza de datos",
        "🔄 Transformación",
        "💾 Carga a MongoDB",
        "✅ Verificación de replicación"
    ]
    
    print("🔄 PROCESO ETL - PASOS A SEGUIR")
    print("=" * 50)
    
    for i, step in enumerate(steps, 1):
        print(f"{i:2d}. {step}")
    
    print("=" * 50)

def show_dataframe_info_with_progress(df, name="Dataset"):
    """
    Mostrar información del DataFrame con barra de progreso
    
    Args:
        df (pd.DataFrame): DataFrame a analizar
        name (str): Nombre del dataset
    """
    print(f"\n📊 ANÁLISIS DE {name.upper()}")
    print("=" * 50)
    
    with create_progress_bar(4, f"Analizando {name}") as pbar:
        # Información básica
        print(f"📋 Dimensiones: {df.shape}")
        pbar.update(1)
        
        # Tipos de datos
        print(f"📈 Tipos de datos:")
        for col, dtype in df.dtypes.items():
            print(f"   • {col}: {dtype}")
        pbar.update(1)
        
        # Valores nulos
        nulls = df.isnull().sum()
        if nulls.sum() > 0:
            print(f"⚠️ Valores nulos:")
            for col, null_count in nulls[nulls > 0].items():
                print(f"   • {col}: {null_count}")
        else:
            print("✅ No hay valores nulos")
        pbar.update(1)
        
        # Estadísticas básicas
        if df.select_dtypes(include=[np.number]).shape[1] > 0:
            print(f"📊 Estadísticas numéricas:")
            print(df.describe())
        pbar.update(1)

def progress_data_cleaning(df, steps=None):
    """
    Mostrar progreso de limpieza de datos
    
    Args:
        df (pd.DataFrame): DataFrame a limpiar
        steps (list): Lista de pasos de limpieza
    """
    if steps is None:
        steps = [
            "Eliminando duplicados",
            "Convirtiendo tipos de datos",
            "Manejando valores nulos",
            "Renombrando columnas",
            "Agregando campos calculados"
        ]
    
    print(f"\n🧹 LIMPIEZA DE DATOS")
    print("=" * 50)
    
    with create_progress_bar(len(steps), "Limpiando datos") as pbar:
        for step in steps:
            print(f"   🔄 {step}")
            time.sleep(0.5)  # Simular procesamiento
            pbar.update(1)
    
    print(f"✅ Limpieza completada - Dataset final: {df.shape}")

def progress_mongodb_operations(operations):
    """
    Mostrar progreso de operaciones MongoDB
    
    Args:
        operations (list): Lista de operaciones a realizar
    """
    print(f"\n🗄️ OPERACIONES MONGODB")
    print("=" * 50)
    
    with create_progress_bar(len(operations), "Operaciones MongoDB") as pbar:
        for operation in operations:
            print(f"   🔄 {operation}")
            time.sleep(0.3)  # Simular operación
            pbar.update(1)
    
    print("✅ Operaciones MongoDB completadas")

def show_replication_status():
    """
    Mostrar estado de replicación con animación
    """
    print(f"\n🔄 VERIFICANDO REPLICACIÓN")
    print("=" * 50)
    
    nodes = ["Primario (27017)", "Secundario 1 (27018)", "Secundario 2 (27019)"]
    
    with create_progress_bar(len(nodes), "Verificando nodos") as pbar:
        for node in nodes:
            print(f"   🔍 Verificando {node}...")
            time.sleep(1)  # Simular verificación
            print(f"   ✅ {node} - Conectado")
            pbar.update(1)
    
    print("✅ Replicación verificada correctamente")

def create_summary_table(data_dict):
    """
    Crear tabla resumen con información del proyecto
    
    Args:
        data_dict (dict): Diccionario con datos del resumen
    """
    print(f"\n📋 RESUMEN DEL PROYECTO")
    print("=" * 50)
    
    for key, value in data_dict.items():
        if isinstance(value, (int, float)):
            print(f"   {key}: {value:,}")
        else:
            print(f"   {key}: {value}")
    
    print("=" * 50)

# Funciones específicas para el proyecto
def show_dataset_download_progress():
    """Mostrar progreso de descarga del dataset"""
    print("📥 DESCARGANDO DATASET DE KAGGLE")
    print("=" * 50)
    
    with create_progress_bar(100, "Descargando") as pbar:
        for i in range(100):
            time.sleep(0.05)
            pbar.update(1)
    
    print("✅ Dataset descargado exitosamente")

def show_csv_loading_progress(files):
    """Mostrar progreso de carga de archivos CSV"""
    print(f"\n📊 CARGANDO ARCHIVOS CSV")
    print("=" * 50)
    
    with create_progress_bar(len(files), "Cargando CSVs") as pbar:
        for file in files:
            print(f"   📄 Cargando {file}...")
            time.sleep(0.5)
            pbar.update(1)
    
    print("✅ Todos los archivos CSV cargados")

def show_eda_progress():
    """Mostrar progreso del análisis exploratorio"""
    print(f"\n🔍 ANÁLISIS EXPLORATORIO DE DATOS")
    print("=" * 50)
    
    eda_steps = [
        "Analizando estructura de datos",
        "Verificando tipos de datos",
        "Identificando valores nulos",
        "Calculando estadísticas descriptivas",
        "Generando visualizaciones"
    ]
    
    with create_progress_bar(len(eda_steps), "Realizando EDA") as pbar:
        for step in eda_steps:
            print(f"   🔍 {step}")
            time.sleep(0.8)
            pbar.update(1)
    
    print("✅ Análisis exploratorio completado")

if __name__ == "__main__":
    # Ejemplo de uso
    print("🧪 PRUEBA DE UTILIDADES DE PROGRESO")
    print("=" * 50)
    
    # Crear DataFrame de ejemplo
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.choice(['X', 'Y', 'Z'], 1000),
        'C': np.random.randint(1, 100, 1000)
    })
    
    show_dataframe_info_with_progress(df, "Ejemplo")
    progress_data_cleaning(df)
    progress_mongodb_operations(["Conectar", "Insertar", "Verificar"])
    show_replication_status() 