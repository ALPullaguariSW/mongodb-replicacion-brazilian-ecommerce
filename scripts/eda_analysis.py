#!/usr/bin/env python3
"""
Análisis Exploratorio de Datos (EDA) - Perspectiva de DBA
Dataset: Brazilian E-Commerce Public Dataset by Olist
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class EDAAnalyzer:
    def __init__(self, data_path='data/raw'):
        self.data_path = Path(data_path)
        self.datasets = {}
        self.analysis_results = {}
        
    def load_all_datasets(self):
        """Cargar todos los datasets CSV"""
        print("📊 CARGANDO DATASETS...")
        print("="*60)
        
        csv_files = list(self.data_path.glob('*.csv'))
        
        for file_path in csv_files:
            filename = file_path.name
            print(f"📥 Cargando {filename}...")
            
            try:
                # Leer con manejo de errores
                df = pd.read_csv(file_path, low_memory=False)
                self.datasets[filename] = df
                print(f"✅ {filename}: {len(df):,} filas, {len(df.columns)} columnas")
                
            except Exception as e:
                print(f"❌ Error cargando {filename}: {e}")
        
        print(f"\n📁 Total datasets cargados: {len(self.datasets)}")
        
    def analyze_dataset_structure(self):
        """Análisis de estructura de cada dataset"""
        print("\n🔍 ANÁLISIS DE ESTRUCTURA DE DATASETS")
        print("="*60)
        
        for filename, df in self.datasets.items():
            print(f"\n📋 {filename}")
            print("-" * 40)
            
            # Información básica
            print(f"📊 Filas: {len(df):,}")
            print(f"📊 Columnas: {len(df.columns)}")
            print(f"📊 Memoria: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            
            # Tipos de datos
            print(f"\n📝 Tipos de datos:")
            for col, dtype in df.dtypes.items():
                print(f"  • {col}: {dtype}")
            
            # Valores únicos por columna
            print(f"\n🔑 Valores únicos por columna:")
            for col in df.columns:
                unique_count = df[col].nunique()
                print(f"  • {col}: {unique_count:,} únicos")
                
            # Valores nulos
            null_counts = df.isnull().sum()
            if null_counts.sum() > 0:
                print(f"\n⚠️ Valores nulos:")
                for col, null_count in null_counts[null_counts > 0].items():
                    percentage = (null_count / len(df)) * 100
                    print(f"  • {col}: {null_count:,} ({percentage:.2f}%)")
            else:
                print(f"\n✅ Sin valores nulos")
    
    def analyze_data_quality(self):
        """Análisis de calidad de datos"""
        print("\n🔍 ANÁLISIS DE CALIDAD DE DATOS")
        print("="*60)
        
        quality_report = {}
        
        for filename, df in self.datasets.items():
            print(f"\n📋 {filename}")
            print("-" * 40)
            
            report = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
                'duplicate_rows': df.duplicated().sum(),
                'duplicate_percentage': (df.duplicated().sum() / len(df)) * 100
            }
            
            quality_report[filename] = report
            
            print(f"📊 Filas totales: {report['total_rows']:,}")
            print(f"📊 Porcentaje de nulos: {report['null_percentage']:.2f}%")
            print(f"📊 Filas duplicadas: {report['duplicate_rows']:,} ({report['duplicate_percentage']:.2f}%)")
            
            # Análisis de columnas específicas
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Para columnas de texto
                    empty_strings = (df[col] == '').sum()
                    if empty_strings > 0:
                        print(f"  ⚠️ {col}: {empty_strings:,} strings vacíos")
        
        self.analysis_results['quality_report'] = quality_report
    
    def analyze_relationships(self):
        """Análisis de relaciones entre datasets"""
        print("\n🔗 ANÁLISIS DE RELACIONES ENTRE DATASETS")
        print("="*60)
        
        # Identificar claves primarias y foráneas
        relationships = {
            'customers': {
                'primary_key': 'customer_id',
                'foreign_keys': ['customer_unique_id']
            },
            'orders': {
                'primary_key': 'order_id',
                'foreign_keys': ['customer_id']
            },
            'order_items': {
                'primary_key': ['order_id', 'order_item_id'],
                'foreign_keys': ['order_id', 'product_id', 'seller_id']
            },
            'products': {
                'primary_key': 'product_id',
                'foreign_keys': ['product_category_name']
            },
            'sellers': {
                'primary_key': 'seller_id',
                'foreign_keys': []
            },
            'payments': {
                'primary_key': ['order_id', 'payment_sequential'],
                'foreign_keys': ['order_id']
            },
            'reviews': {
                'primary_key': 'review_id',
                'foreign_keys': ['order_id']
            }
        }
        
        print("🔑 CLAVES PRIMARIAS Y FORÁNEAS:")
        for dataset_name, keys in relationships.items():
            print(f"\n📋 {dataset_name}:")
            print(f"  🔑 PK: {keys['primary_key']}")
            print(f"  🔗 FK: {keys['foreign_keys']}")
        
        # Verificar integridad referencial
        print(f"\n🔍 VERIFICACIÓN DE INTEGRIDAD REFERENCIAL:")
        
        # Verificar customer_id en orders vs customers
        if 'olist_customers_dataset.csv' in self.datasets and 'olist_orders_dataset.csv' in self.datasets:
            customers_df = self.datasets['olist_customers_dataset.csv']
            orders_df = self.datasets['olist_orders_dataset.csv']
            
            customers_ids = set(customers_df['customer_id'])
            orders_customer_ids = set(orders_df['customer_id'])
            
            missing_customers = orders_customer_ids - customers_ids
            orphan_customers = customers_ids - orders_customer_ids
            
            print(f"\n📊 Relación Orders-Customers:")
            print(f"  • Customer IDs en orders: {len(orders_customer_ids):,}")
            print(f"  • Customer IDs en customers: {len(customers_ids):,}")
            print(f"  • Orders sin customer: {len(missing_customers):,}")
            print(f"  • Customers sin orders: {len(orphan_customers):,}")
    
    def analyze_business_insights(self):
        """Análisis de insights de negocio"""
        print("\n💼 ANÁLISIS DE INSIGHTS DE NEGOCIO")
        print("="*60)
        
        # Análisis temporal
        if 'olist_orders_dataset.csv' in self.datasets:
            orders_df = self.datasets['olist_orders_dataset.csv']
            
            # Convertir fechas
            orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
            
            print(f"\n📅 ANÁLISIS TEMPORAL:")
            print(f"  • Período: {orders_df['order_purchase_timestamp'].min()} a {orders_df['order_purchase_timestamp'].max()}")
            print(f"  • Total días: {(orders_df['order_purchase_timestamp'].max() - orders_df['order_purchase_timestamp'].min()).days}")
            
            # Análisis por estado
            print(f"\n🗺️ ANÁLISIS GEOGRÁFICO:")
            if 'olist_customers_dataset.csv' in self.datasets:
                customers_df = self.datasets['olist_customers_dataset.csv']
                state_counts = customers_df['customer_state'].value_counts()
                print(f"  • Estados con más clientes:")
                for state, count in state_counts.head(5).items():
                    print(f"    - {state}: {count:,} clientes")
        
        # Análisis de productos
        if 'olist_products_dataset.csv' in self.datasets:
            products_df = self.datasets['olist_products_dataset.csv']
            
            print(f"\n📦 ANÁLISIS DE PRODUCTOS:")
            print(f"  • Total productos: {len(products_df):,}")
            print(f"  • Categorías únicas: {products_df['product_category_name'].nunique():,}")
            
            # Categorías más populares
            category_counts = products_df['product_category_name'].value_counts()
            print(f"  • Top 5 categorías:")
            for category, count in category_counts.head(5).items():
                print(f"    - {category}: {count:,} productos")
    
    def generate_recommendations(self):
        """Generar recomendaciones para el ETL"""
        print("\n💡 RECOMENDACIONES PARA ETL")
        print("="*60)
        
        recommendations = []
        
        # Recomendaciones de limpieza
        print("🧹 LIMPIEZA DE DATOS:")
        print("  • Eliminar filas duplicadas")
        print("  • Manejar valores nulos según el contexto")
        print("  • Normalizar formatos de fecha")
        print("  • Validar códigos postales")
        
        # Recomendaciones de transformación
        print("\n🔄 TRANSFORMACIONES:")
        print("  • Crear índices para claves primarias y foráneas")
        print("  • Agregar campos calculados (total_ventas, etc.)")
        print("  • Normalizar categorías de productos")
        print("  • Crear dimensiones de tiempo")
        
        # Recomendaciones de MongoDB
        print("\n🗄️ ESTRUCTURA MONGODB:")
        print("  • Colección principal: orders (con embedded items)")
        print("  • Colecciones separadas: customers, products, sellers")
        print("  • Índices en campos de consulta frecuente")
        print("  • Considerar agregaciones pre-calculadas")
        
        recommendations.extend([
            "Eliminar duplicados antes del procesamiento",
            "Crear índices en MongoDB para optimizar consultas",
            "Implementar validación de datos en el ETL",
            "Considerar particionamiento por fecha",
            "Agregar campos de auditoría (created_at, updated_at)"
        ])
        
        self.analysis_results['recommendations'] = recommendations
    
    def save_analysis_report(self):
        """Guardar reporte de análisis"""
        report_path = Path('data/processed/eda_report.json')
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Preparar datos para JSON
        quality_report_clean = {}
        for filename, report in self.analysis_results.get('quality_report', {}).items():
            quality_report_clean[filename] = {
                'total_rows': int(report['total_rows']),
                'total_columns': int(report['total_columns']),
                'null_percentage': float(report['null_percentage']),
                'duplicate_rows': int(report['duplicate_rows']),
                'duplicate_percentage': float(report['duplicate_percentage'])
            }
        
        json_data = {
            'analysis_date': datetime.now().isoformat(),
            'datasets_analyzed': list(self.datasets.keys()),
            'quality_report': quality_report_clean,
            'recommendations': self.analysis_results.get('recommendations', [])
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Reporte guardado en: {report_path}")
    
    def run_full_analysis(self):
        """Ejecutar análisis completo"""
        print("🎯 ANÁLISIS EXPLORATORIO DE DATOS - PERSPECTIVA DBA")
        print("="*80)
        
        # Cargar datos
        self.load_all_datasets()
        
        # Análisis de estructura
        self.analyze_dataset_structure()
        
        # Análisis de calidad
        self.analyze_data_quality()
        
        # Análisis de relaciones
        self.analyze_relationships()
        
        # Análisis de negocio
        self.analyze_business_insights()
        
        # Recomendaciones
        self.generate_recommendations()
        
        # Guardar reporte
        self.save_analysis_report()
        
        print("\n✅ ANÁLISIS COMPLETADO")
        print("📝 Próximos pasos:")
        print("   1. Revisar el reporte de calidad")
        print("   2. Implementar el proceso ETL")
        print("   3. Diseñar la estructura de MongoDB")

if __name__ == "__main__":
    analyzer = EDAAnalyzer()
    analyzer.run_full_analysis()
