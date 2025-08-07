#!/usr/bin/env python3
"""
Proceso ETL (Extract, Transform, Load) - Dataset Brazilian E-Commerce
Basado en el análisis EDA previo
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class ETLProcessor:
    def __init__(self, raw_data_path='data/raw', processed_data_path='data/processed'):
        self.raw_data_path = Path(raw_data_path)
        self.processed_data_path = Path(processed_data_path)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        self.datasets = {}
        self.processed_datasets = {}
        self.etl_report = {
            'start_time': datetime.now().isoformat(),
            'processing_steps': [],
            'data_quality_improvements': {},
            'final_statistics': {}
        }
        
    def load_raw_datasets(self):
        """Cargar datasets originales"""
        print("📥 CARGANDO DATASETS ORIGINALES...")
        print("="*60)
        
        csv_files = list(self.raw_data_path.glob('*.csv'))
        
        for file_path in csv_files:
            filename = file_path.name
            print(f"📥 Cargando {filename}...")
            
            try:
                df = pd.read_csv(file_path, low_memory=False)
                self.datasets[filename] = df
                print(f"✅ {filename}: {len(df):,} filas")
                
            except Exception as e:
                print(f"❌ Error cargando {filename}: {e}")
        
        print(f"📁 Total datasets cargados: {len(self.datasets)}")
        self.etl_report['processing_steps'].append("Carga de datasets originales completada")
    
    def clean_geolocation_data(self):
        """Limpieza del dataset de geolocalización - eliminar duplicados"""
        print("\n🧹 LIMPIEZA DE DATOS DE GEOLOCALIZACIÓN")
        print("="*60)
        
        if 'olist_geolocation_dataset.csv' in self.datasets:
            df = self.datasets['olist_geolocation_dataset.csv'].copy()
            
            print(f"📊 Antes de limpieza: {len(df):,} filas")
            
            # Eliminar duplicados
            df_clean = df.drop_duplicates()
            
            print(f"📊 Después de limpieza: {len(df_clean):,} filas")
            print(f"🗑️ Duplicados eliminados: {len(df) - len(df_clean):,}")
            
            # Validar códigos postales brasileños (deben ser 5 dígitos)
            invalid_zipcodes = df_clean[
                (df_clean['geolocation_zip_code_prefix'] < 10000) | 
                (df_clean['geolocation_zip_code_prefix'] > 99999)
            ]
            
            if len(invalid_zipcodes) > 0:
                print(f"⚠️ Códigos postales inválidos encontrados: {len(invalid_zipcodes)}")
                df_clean = df_clean[
                    (df_clean['geolocation_zip_code_prefix'] >= 10000) & 
                    (df_clean['geolocation_zip_code_prefix'] <= 99999)
                ]
                print(f"📊 Después de validación: {len(df_clean):,} filas")
            
            self.processed_datasets['geolocation'] = df_clean
            self.etl_report['data_quality_improvements']['geolocation'] = {
                'original_rows': len(df),
                'final_rows': len(df_clean),
                'duplicates_removed': len(df) - len(df_clean),
                'invalid_zipcodes_removed': len(invalid_zipcodes) if len(invalid_zipcodes) > 0 else 0
            }
    
    def clean_orders_data(self):
        """Limpieza y transformación del dataset de órdenes"""
        print("\n🧹 LIMPIEZA Y TRANSFORMACIÓN DE ÓRDENES")
        print("="*60)
        
        if 'olist_orders_dataset.csv' in self.datasets:
            df = self.datasets['olist_orders_dataset.csv'].copy()
            
            print(f"📊 Filas originales: {len(df):,}")
            
            # Convertir fechas
            date_columns = [
                'order_purchase_timestamp',
                'order_approved_at',
                'order_delivered_carrier_date',
                'order_delivered_customer_date',
                'order_estimated_delivery_date'
            ]
            
            for col in date_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Crear dimensiones de tiempo
            df['order_year'] = df['order_purchase_timestamp'].dt.year
            df['order_month'] = df['order_purchase_timestamp'].dt.month
            df['order_day'] = df['order_purchase_timestamp'].dt.day
            df['order_weekday'] = df['order_purchase_timestamp'].dt.day_name()
            df['order_quarter'] = df['order_purchase_timestamp'].dt.quarter
            
            # Calcular tiempo de entrega
            df['delivery_time_days'] = (
                df['order_delivered_customer_date'] - df['order_purchase_timestamp']
            ).dt.days
            
            # Manejar valores nulos en fechas de entrega
            df['delivery_time_days'] = df['delivery_time_days'].fillna(-1)  # -1 para no entregados
            
            # Crear campo de estado de entrega
            df['delivery_status'] = df['order_status'].map({
                'delivered': 'Entregado',
                'shipped': 'Enviado',
                'processing': 'Procesando',
                'canceled': 'Cancelado',
                'unavailable': 'No disponible',
                'invoiced': 'Facturado',
                'approved': 'Aprobado',
                'created': 'Creado'
            })
            
            print(f"📊 Filas procesadas: {len(df):,}")
            print(f"📅 Rango temporal: {df['order_purchase_timestamp'].min()} a {df['order_purchase_timestamp'].max()}")
            
            self.processed_datasets['orders'] = df
            self.etl_report['data_quality_improvements']['orders'] = {
                'original_rows': len(self.datasets['olist_orders_dataset.csv']),
                'final_rows': len(df),
                'date_columns_processed': len(date_columns),
                'time_dimensions_added': 5
            }
    
    def clean_products_data(self):
        """Limpieza y transformación del dataset de productos"""
        print("\n🧹 LIMPIEZA Y TRANSFORMACIÓN DE PRODUCTOS")
        print("="*60)
        
        if 'olist_products_dataset.csv' in self.datasets:
            df = self.datasets['olist_products_dataset.csv'].copy()
            
            print(f"📊 Filas originales: {len(df):,}")
            
            # Manejar valores nulos en categorías
            df['product_category_name'] = df['product_category_name'].fillna('categoria_nao_informada')
            
            # Normalizar categorías (convertir a minúsculas y reemplazar espacios)
            df['product_category_name_normalized'] = df['product_category_name'].str.lower().str.replace(' ', '_')
            
            # Crear dimensiones de producto
            df['product_volume_cm3'] = df['product_length_cm'] * df['product_height_cm'] * df['product_width_cm']
            df['product_volume_cm3'] = df['product_volume_cm3'].fillna(0)
            
            # Categorizar productos por peso
            df['weight_category'] = pd.cut(
                df['product_weight_g'],
                bins=[0, 100, 500, 1000, 5000, float('inf')],
                labels=['Muy ligero', 'Ligero', 'Mediano', 'Pesado', 'Muy pesado'],
                include_lowest=True
            )
            
            # Categorizar productos por dimensiones
            df['size_category'] = pd.cut(
                df['product_volume_cm3'],
                bins=[0, 1000, 10000, 100000, float('inf')],
                labels=['Pequeño', 'Mediano', 'Grande', 'Muy grande'],
                include_lowest=True
            )
            
            print(f"📊 Filas procesadas: {len(df):,}")
            print(f"📦 Categorías únicas: {df['product_category_name'].nunique()}")
            
            self.processed_datasets['products'] = df
            self.etl_report['data_quality_improvements']['products'] = {
                'original_rows': len(self.datasets['olist_products_dataset.csv']),
                'final_rows': len(df),
                'null_categories_filled': df['product_category_name'].isnull().sum(),
                'new_dimensions_added': 4
            }
    
    def clean_customers_data(self):
        """Limpieza y transformación del dataset de clientes"""
        print("\n🧹 LIMPIEZA Y TRANSFORMACIÓN DE CLIENTES")
        print("="*60)
        
        if 'olist_customers_dataset.csv' in self.datasets:
            df = self.datasets['olist_customers_dataset.csv'].copy()
            
            print(f"📊 Filas originales: {len(df):,}")
            
            # Validar códigos postales
            invalid_zipcodes = df[
                (df['customer_zip_code_prefix'] < 10000) | 
                (df['customer_zip_code_prefix'] > 99999)
            ]
            
            if len(invalid_zipcodes) > 0:
                print(f"⚠️ Códigos postales inválidos: {len(invalid_zipcodes)}")
                df = df[
                    (df['customer_zip_code_prefix'] >= 10000) & 
                    (df['customer_zip_code_prefix'] <= 99999)
                ]
            
            # Normalizar nombres de ciudades y estados
            df['customer_city_normalized'] = df['customer_city'].str.title()
            df['customer_state_normalized'] = df['customer_state'].str.upper()
            
            # Crear región geográfica
            region_mapping = {
                'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
                'RS': 'Sul', 'SC': 'Sul', 'PR': 'Sul',
                'BA': 'Nordeste', 'PE': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste',
                'PB': 'Nordeste', 'RN': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste',
                'PI': 'Nordeste',
                'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
                'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'AC': 'Norte', 'RR': 'Norte', 'AP': 'Norte', 'TO': 'Norte'
            }
            
            df['customer_region'] = df['customer_state_normalized'].map(region_mapping)
            
            print(f"📊 Filas procesadas: {len(df):,}")
            print(f"🗺️ Regiones: {df['customer_region'].value_counts().to_dict()}")
            
            self.processed_datasets['customers'] = df
            self.etl_report['data_quality_improvements']['customers'] = {
                'original_rows': len(self.datasets['olist_customers_dataset.csv']),
                'final_rows': len(df),
                'invalid_zipcodes_removed': len(invalid_zipcodes) if len(invalid_zipcodes) > 0 else 0,
                'new_dimensions_added': 3
            }
    
    def clean_sellers_data(self):
        """Limpieza y transformación del dataset de vendedores"""
        print("\n🧹 LIMPIEZA Y TRANSFORMACIÓN DE VENDEDORES")
        print("="*60)
        
        if 'olist_sellers_dataset.csv' in self.datasets:
            df = self.datasets['olist_sellers_dataset.csv'].copy()
            
            print(f"📊 Filas originales: {len(df):,}")
            
            # Validar códigos postales
            invalid_zipcodes = df[
                (df['seller_zip_code_prefix'] < 10000) | 
                (df['seller_zip_code_prefix'] > 99999)
            ]
            
            if len(invalid_zipcodes) > 0:
                print(f"⚠️ Códigos postales inválidos: {len(invalid_zipcodes)}")
                df = df[
                    (df['seller_zip_code_prefix'] >= 10000) & 
                    (df['seller_zip_code_prefix'] <= 99999)
                ]
            
            # Normalizar nombres de ciudades y estados
            df['seller_city_normalized'] = df['seller_city'].str.title()
            df['seller_state_normalized'] = df['seller_state'].str.upper()
            
            # Crear región geográfica (usar el mismo mapping que customers)
            region_mapping = {
                'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
                'RS': 'Sul', 'SC': 'Sul', 'PR': 'Sul',
                'BA': 'Nordeste', 'PE': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste',
                'PB': 'Nordeste', 'RN': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste',
                'PI': 'Nordeste',
                'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
                'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'AC': 'Norte', 'RR': 'Norte', 'AP': 'Norte', 'TO': 'Norte'
            }
            
            df['seller_region'] = df['seller_state_normalized'].map(region_mapping)
            
            print(f"📊 Filas procesadas: {len(df):,}")
            print(f"🗺️ Regiones: {df['seller_region'].value_counts().to_dict()}")
            
            self.processed_datasets['sellers'] = df
            self.etl_report['data_quality_improvements']['sellers'] = {
                'original_rows': len(self.datasets['olist_sellers_dataset.csv']),
                'final_rows': len(df),
                'invalid_zipcodes_removed': len(invalid_zipcodes) if len(invalid_zipcodes) > 0 else 0,
                'new_dimensions_added': 3
            }
    
    def clean_order_items_data(self):
        """Limpieza y transformación del dataset de items de orden"""
        print("\n🧹 LIMPIEZA Y TRANSFORMACIÓN DE ITEMS DE ORDEN")
        print("="*60)
        
        if 'olist_order_items_dataset.csv' in self.datasets:
            df = self.datasets['olist_order_items_dataset.csv'].copy()
            
            print(f"📊 Filas originales: {len(df):,}")
            
            # Convertir fechas
            df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
            
            # Crear campos calculados
            df['total_item_value'] = df['price'] + df['freight_value']
            df['freight_percentage'] = (df['freight_value'] / df['price'] * 100).fillna(0)
            
            # Categorizar por valor
            df['value_category'] = pd.cut(
                df['total_item_value'],
                bins=[0, 50, 100, 200, 500, float('inf')],
                labels=['Muy bajo', 'Bajo', 'Medio', 'Alto', 'Muy alto'],
                include_lowest=True
            )
            
            # Categorizar por porcentaje de flete
            df['freight_category'] = pd.cut(
                df['freight_percentage'],
                bins=[0, 10, 25, 50, 100, float('inf')],
                labels=['Muy bajo', 'Bajo', 'Medio', 'Alto', 'Muy alto'],
                include_lowest=True
            )
            
            print(f"📊 Filas procesadas: {len(df):,}")
            print(f"💰 Valor total items: R$ {df['total_item_value'].sum():,.2f}")
            
            self.processed_datasets['order_items'] = df
            self.etl_report['data_quality_improvements']['order_items'] = {
                'original_rows': len(self.datasets['olist_order_items_dataset.csv']),
                'final_rows': len(df),
                'calculated_fields_added': 4
            }
    
    def clean_payments_data(self):
        """Limpieza y transformación del dataset de pagos"""
        print("\n🧹 LIMPIEZA Y TRANSFORMACIÓN DE PAGOS")
        print("="*60)
        
        if 'olist_order_payments_dataset.csv' in self.datasets:
            df = self.datasets['olist_order_payments_dataset.csv'].copy()
            
            print(f"📊 Filas originales: {len(df):,}")
            
            # Normalizar tipos de pago
            payment_mapping = {
                'credit_card': 'Tarjeta de crédito',
                'boleto': 'Boleto bancario',
                'voucher': 'Vale',
                'debit_card': 'Tarjeta de débito',
                'not_defined': 'No definido'
            }
            
            df['payment_type_normalized'] = df['payment_type'].map(payment_mapping)
            
            # Categorizar por valor de pago
            df['payment_value_category'] = pd.cut(
                df['payment_value'],
                bins=[0, 50, 100, 200, 500, 1000, float('inf')],
                labels=['Muy bajo', 'Bajo', 'Medio', 'Alto', 'Muy alto', 'Premium'],
                include_lowest=True
            )
            
            # Categorizar por número de cuotas
            df['installments_category'] = pd.cut(
                df['payment_installments'],
                bins=[0, 1, 3, 6, 12, float('inf')],
                labels=['Pago único', 'Corto plazo', 'Mediano plazo', 'Largo plazo', 'Muy largo plazo'],
                include_lowest=True
            )
            
            print(f"📊 Filas procesadas: {len(df):,}")
            print(f"💳 Tipos de pago: {df['payment_type_normalized'].value_counts().to_dict()}")
            
            self.processed_datasets['payments'] = df
            self.etl_report['data_quality_improvements']['payments'] = {
                'original_rows': len(self.datasets['olist_order_payments_dataset.csv']),
                'final_rows': len(df),
                'payment_types_normalized': len(payment_mapping),
                'new_categories_added': 2
            }
    
    def clean_reviews_data(self):
        """Limpieza y transformación del dataset de reviews"""
        print("\n🧹 LIMPIEZA Y TRANSFORMACIÓN DE REVIEWS")
        print("="*60)
        
        if 'olist_order_reviews_dataset.csv' in self.datasets:
            df = self.datasets['olist_order_reviews_dataset.csv'].copy()
            
            print(f"📊 Filas originales: {len(df):,}")
            
            # Convertir fechas
            df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
            df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
            
            # Manejar comentarios vacíos
            df['has_comment_title'] = df['review_comment_title'].notna() & (df['review_comment_title'] != '')
            df['has_comment_message'] = df['review_comment_message'].notna() & (df['review_comment_message'] != '')
            
            # Categorizar por score
            score_mapping = {
                1: 'Muy malo',
                2: 'Malo',
                3: 'Regular',
                4: 'Bueno',
                5: 'Excelente'
            }
            
            df['review_score_category'] = df['review_score'].map(score_mapping)
            
            # Calcular tiempo de respuesta
            df['response_time_hours'] = (
                df['review_answer_timestamp'] - df['review_creation_date']
            ).dt.total_seconds() / 3600
            
            df['response_time_hours'] = df['response_time_hours'].fillna(-1)  # -1 para sin respuesta
            
            print(f"📊 Filas procesadas: {len(df):,}")
            print(f"⭐ Score promedio: {df['review_score'].mean():.2f}")
            print(f"💬 Reviews con comentarios: {df['has_comment_message'].sum():,}")
            
            self.processed_datasets['reviews'] = df
            self.etl_report['data_quality_improvements']['reviews'] = {
                'original_rows': len(self.datasets['olist_order_reviews_dataset.csv']),
                'final_rows': len(df),
                'reviews_with_comments': df['has_comment_message'].sum(),
                'new_dimensions_added': 4
            }
    
    def create_aggregated_datasets(self):
        """Crear datasets agregados para análisis"""
        print("\n📊 CREANDO DATASETS AGREGADOS")
        print("="*60)
        
        # Agregar datos de clientes a órdenes
        if 'orders' in self.processed_datasets and 'customers' in self.processed_datasets:
            orders_df = self.processed_datasets['orders']
            customers_df = self.processed_datasets['customers']
            
            # Merge orders con customers
            orders_with_customers = orders_df.merge(
                customers_df[['customer_id', 'customer_city_normalized', 'customer_state_normalized', 'customer_region']],
                on='customer_id',
                how='left'
            )
            
            self.processed_datasets['orders_with_customers'] = orders_with_customers
            print(f"✅ Orders con datos de clientes: {len(orders_with_customers):,} filas")
        
        # Agregar datos de productos a items
        if 'order_items' in self.processed_datasets and 'products' in self.processed_datasets:
            items_df = self.processed_datasets['order_items']
            products_df = self.processed_datasets['products']
            
            # Merge items con products
            items_with_products = items_df.merge(
                products_df[['product_id', 'product_category_name', 'product_category_name_normalized', 'weight_category', 'size_category']],
                on='product_id',
                how='left'
            )
            
            self.processed_datasets['items_with_products'] = items_with_products
            print(f"✅ Items con datos de productos: {len(items_with_products):,} filas")
    
    def save_processed_datasets(self):
        """Guardar datasets procesados"""
        print("\n💾 GUARDANDO DATASETS PROCESADOS")
        print("="*60)
        
        for name, df in self.processed_datasets.items():
            output_path = self.processed_data_path / f"{name}.csv"
            df.to_csv(output_path, index=False)
            print(f"💾 {name}.csv: {len(df):,} filas guardadas")
        
        # Guardar reporte ETL
        self.etl_report['end_time'] = datetime.now().isoformat()
        self.etl_report['total_processing_time'] = float((
            datetime.fromisoformat(self.etl_report['end_time']) - 
            datetime.fromisoformat(self.etl_report['start_time'])
        ).total_seconds())
        
        # Limpiar datos para JSON
        def clean_for_json(obj):
            if isinstance(obj, dict):
                return {k: clean_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_for_json(item) for item in obj]
            elif isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            elif pd.isna(obj):
                return None
            else:
                return obj
        
        clean_report = clean_for_json(self.etl_report)
        
        report_path = self.processed_data_path / 'etl_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(clean_report, f, indent=2, ensure_ascii=False)
        
        print(f"📋 Reporte ETL guardado en: {report_path}")
    
    def generate_final_statistics(self):
        """Generar estadísticas finales"""
        print("\n📊 ESTADÍSTICAS FINALES")
        print("="*60)
        
        stats = {}
        
        for name, df in self.processed_datasets.items():
            stats[name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_mb': df.memory_usage(deep=True).sum() / 1024**2
            }
        
        total_rows = sum(stats[name]['rows'] for name in stats)
        total_memory = sum(stats[name]['memory_mb'] for name in stats)
        
        print(f"📊 Total datasets procesados: {len(self.processed_datasets)}")
        print(f"📊 Total filas: {total_rows:,}")
        print(f"📊 Memoria total: {total_memory:.2f} MB")
        
        for name, stat in stats.items():
            print(f"  • {name}: {stat['rows']:,} filas, {stat['columns']} columnas, {stat['memory_mb']:.2f} MB")
        
        self.etl_report['final_statistics'] = {
            'total_datasets': len(self.processed_datasets),
            'total_rows': total_rows,
            'total_memory_mb': total_memory,
            'dataset_details': stats
        }
    
    def run_full_etl(self):
        """Ejecutar proceso ETL completo"""
        print("🎯 PROCESO ETL COMPLETO - DATASET BRAZILIAN E-COMMERCE")
        print("="*80)
        
        # Cargar datos originales
        self.load_raw_datasets()
        
        # Limpiar y transformar cada dataset
        self.clean_geolocation_data()
        self.clean_orders_data()
        self.clean_products_data()
        self.clean_customers_data()
        self.clean_sellers_data()
        self.clean_order_items_data()
        self.clean_payments_data()
        self.clean_reviews_data()
        
        # Crear datasets agregados
        self.create_aggregated_datasets()
        
        # Generar estadísticas finales
        self.generate_final_statistics()
        
        # Guardar datasets procesados
        self.save_processed_datasets()
        
        print("\n✅ PROCESO ETL COMPLETADO")
        print("📝 Próximos pasos:")
        print("   1. Revisar datasets procesados en data/processed/")
        print("   2. Verificar reporte ETL en etl_report.json")
        print("   3. Proceder con carga a MongoDB")
        print("   4. Implementar consultas CRUD")

if __name__ == "__main__":
    processor = ETLProcessor()
    processor.run_full_etl()
