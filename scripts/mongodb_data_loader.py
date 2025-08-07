#!/usr/bin/env python3
"""
Cargador de Datos a MongoDB
Dataset: Brazilian E-Commerce (datos procesados)
Transforma CSV a documentos JSON y los carga a MongoDB
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import warnings
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, DuplicateKeyError
import time
warnings.filterwarnings('ignore')

class MongoDBDataLoader:
    def __init__(self, processed_data_path='data/processed', mongodb_uri='mongodb://localhost:27020/'):
        self.processed_data_path = Path(processed_data_path)
        self.mongodb_uri = mongodb_uri
        self.client = None
        self.db = None
        self.datasets = {}
        self.load_report = {
            'start_time': datetime.now().isoformat(),
            'collections_loaded': {},
            'total_documents': 0,
            'errors': []
        }
        
    def connect_to_mongodb(self):
        """Conectar a MongoDB"""
        print("🔌 CONECTANDO A MONGODB...")
        print("="*60)
        
        try:
            # Conectar directamente al nodo primario para carga de datos
            self.client = MongoClient(
                self.mongodb_uri,
                directConnection=True,  # Conexión directa al nodo primario
                serverSelectionTimeoutMS=5000
            )
            # Verificar conexión
            self.client.admin.command('ping')
            print("✅ Conexión exitosa a MongoDB (conexión directa al primario)")
            
            # Crear base de datos
            self.db = self.client['brazilian_ecommerce']
            print(f"📁 Base de datos: {self.db.name}")
            
        except Exception as e:
            print(f"❌ Error conectando a MongoDB: {e}")
            print("💡 Asegúrate de que MongoDB esté ejecutándose en el puerto 27020")
            print("💡 Verifica que el nodo primario esté disponible")
            raise
    
    def clean_existing_collections(self):
        """Limpiar colecciones existentes antes de cargar nuevos datos"""
        print("\n🧹 LIMPIANDO COLECCIONES EXISTENTES...")
        print("="*60)
        
        collections_to_clean = ['orders', 'products', 'customers', 'sellers']
        
        for collection_name in collections_to_clean:
            collection = self.db[collection_name]
            count = collection.count_documents({})
            if count > 0:
                collection.delete_many({})
                print(f"🗑️ {collection_name}: {count:,} documentos eliminados")
            else:
                print(f"✅ {collection_name}: ya está vacía")
        
        print("✅ Limpieza completada")
    
    def load_processed_datasets(self):
        """Cargar datasets procesados"""
        print("\n📥 CARGANDO DATASETS PROCESADOS...")
        print("="*60)
        
        csv_files = list(self.processed_data_path.glob('*.csv'))
        
        for file_path in csv_files:
            if file_path.name != 'geolocation.csv':  # No necesitamos geolocation
                filename = file_path.name.replace('.csv', '')
                print(f"📥 Cargando {filename}...")
                
                try:
                    df = pd.read_csv(file_path, low_memory=False)
                    self.datasets[filename] = df
                    print(f"✅ {filename}: {len(df):,} filas")
                    
                except Exception as e:
                    print(f"❌ Error cargando {filename}: {e}")
        
        print(f"📁 Total datasets cargados: {len(self.datasets)}")
    
    def clean_for_mongodb(self, obj):
        """Limpiar datos para MongoDB (convertir tipos)"""
        if isinstance(obj, dict):
            return {k: self.clean_for_mongodb(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_for_mongodb(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif pd.isna(obj):
            return None
        elif isinstance(obj, str) and obj == '':
            return None
        else:
            return obj
    
    def load_products_collection(self):
        """Cargar colección de productos"""
        print("\n📦 CARGANDO COLECCIÓN PRODUCTS...")
        print("="*60)
        
        if 'products' in self.datasets:
            df = self.datasets['products']
            collection = self.db['products']
            
            # Crear índice único en product_id
            collection.create_index("product_id", unique=True)
            
            documents = []
            for _, row in df.iterrows():
                doc = {
                    'product_id': row['product_id'],
                    'product_category_name': row['product_category_name'],
                    'product_category_name_normalized': row['product_category_name_normalized'],
                    'product_name_lenght': row['product_name_lenght'],
                    'product_description_lenght': row['product_description_lenght'],
                    'product_photos_qty': row['product_photos_qty'],
                    'product_weight_g': row['product_weight_g'],
                    'product_length_cm': row['product_length_cm'],
                    'product_height_cm': row['product_height_cm'],
                    'product_width_cm': row['product_width_cm'],
                    'product_volume_cm3': row['product_volume_cm3'],
                    'weight_category': row['weight_category'],
                    'size_category': row['size_category'],
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                doc = self.clean_for_mongodb(doc)
                documents.append(doc)
            
            # Insertar documentos
            try:
                result = collection.insert_many(documents, ordered=False)
                print(f"✅ {len(result.inserted_ids):,} productos insertados")
                
                self.load_report['collections_loaded']['products'] = {
                    'documents_inserted': len(result.inserted_ids),
                    'collection_size': collection.count_documents({})
                }
                
            except BulkWriteError as e:
                inserted = len(e.details['insertedIds'])
                print(f"⚠️ {inserted:,} productos insertados (algunos duplicados)")
                
                self.load_report['collections_loaded']['products'] = {
                    'documents_inserted': inserted,
                    'collection_size': collection.count_documents({})
                }
    
    def load_customers_collection(self):
        """Cargar colección de clientes"""
        print("\n👥 CARGANDO COLECCIÓN CUSTOMERS...")
        print("="*60)
        
        if 'customers' in self.datasets:
            df = self.datasets['customers']
            collection = self.db['customers']
            
            # Crear índice único en customer_id
            collection.create_index("customer_id", unique=True)
            
            documents = []
            for _, row in df.iterrows():
                doc = {
                    'customer_id': row['customer_id'],
                    'customer_unique_id': row['customer_unique_id'],
                    'customer_zip_code_prefix': row['customer_zip_code_prefix'],
                    'customer_city': row['customer_city'],
                    'customer_state': row['customer_state'],
                    'customer_city_normalized': row['customer_city_normalized'],
                    'customer_state_normalized': row['customer_state_normalized'],
                    'customer_region': row['customer_region'],
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                doc = self.clean_for_mongodb(doc)
                documents.append(doc)
            
            # Insertar documentos
            try:
                result = collection.insert_many(documents, ordered=False)
                print(f"✅ {len(result.inserted_ids):,} clientes insertados")
                
                self.load_report['collections_loaded']['customers'] = {
                    'documents_inserted': len(result.inserted_ids),
                    'collection_size': collection.count_documents({})
                }
                
            except BulkWriteError as e:
                inserted = len(e.details['insertedIds'])
                print(f"⚠️ {inserted:,} clientes insertados (algunos duplicados)")
                
                self.load_report['collections_loaded']['customers'] = {
                    'documents_inserted': inserted,
                    'collection_size': collection.count_documents({})
                }
    
    def load_sellers_collection(self):
        """Cargar colección de vendedores"""
        print("\n🏪 CARGANDO COLECCIÓN SELLERS...")
        print("="*60)
        
        if 'sellers' in self.datasets:
            df = self.datasets['sellers']
            collection = self.db['sellers']
            
            # Crear índice único en seller_id
            collection.create_index("seller_id", unique=True)
            
            documents = []
            for _, row in df.iterrows():
                doc = {
                    'seller_id': row['seller_id'],
                    'seller_zip_code_prefix': row['seller_zip_code_prefix'],
                    'seller_city': row['seller_city'],
                    'seller_state': row['seller_state'],
                    'seller_city_normalized': row['seller_city_normalized'],
                    'seller_state_normalized': row['seller_state_normalized'],
                    'seller_region': row['seller_region'],
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                doc = self.clean_for_mongodb(doc)
                documents.append(doc)
            
            # Insertar documentos
            try:
                result = collection.insert_many(documents, ordered=False)
                print(f"✅ {len(result.inserted_ids):,} vendedores insertados")
                
                self.load_report['collections_loaded']['sellers'] = {
                    'documents_inserted': len(result.inserted_ids),
                    'collection_size': collection.count_documents({})
                }
                
            except BulkWriteError as e:
                inserted = len(e.details['insertedIds'])
                print(f"⚠️ {inserted:,} vendedores insertados (algunos duplicados)")
                
                self.load_report['collections_loaded']['sellers'] = {
                    'documents_inserted': inserted,
                    'collection_size': collection.count_documents({})
                }
    
    def load_orders_collection(self):
        """Cargar colección principal de órdenes con datos anidados (OPTIMIZADO)"""
        print("\n📋 CARGANDO COLECCIÓN ORDERS (OPTIMIZADO)...")
        print("="*60)
        
        if all(key in self.datasets for key in ['orders_with_customers', 'items_with_products', 'payments', 'reviews']):
            orders_df = self.datasets['orders_with_customers']
            items_df = self.datasets['items_with_products']
            payments_df = self.datasets['payments']
            reviews_df = self.datasets['reviews']
            
            collection = self.db['orders']
            
            # OPTIMIZACIÓN 1: Crear índices después de la carga para mayor velocidad
            # collection.create_index("order_id", unique=True)
            # collection.create_index("customer.customer_id")
            # collection.create_index("order_info.order_purchase_timestamp")
            # collection.create_index("customer.customer_state")
            # collection.create_index("customer.customer_region")
            # collection.create_index("order_info.order_status")
            # collection.create_index("time_dimensions.order_year")
            # collection.create_index("time_dimensions.order_month")
            # collection.create_index("order_summary.total_value")
            # collection.create_index("review.review_score")
            
            # OPTIMIZACIÓN 2: Pre-procesar DataFrames para evitar búsquedas repetitivas
            print("🔄 Pre-procesando datos para optimización...")
            
            # Crear diccionarios para acceso O(1) en lugar de filtros O(n)
            items_dict = {}
            for _, item in items_df.iterrows():
                order_id = item['order_id']
                if order_id not in items_dict:
                    items_dict[order_id] = []
                items_dict[order_id].append(item.to_dict())
            
            payments_dict = {}
            for _, payment in payments_df.iterrows():
                order_id = payment['order_id']
                if order_id not in payments_dict:
                    payments_dict[order_id] = []
                payments_dict[order_id].append(payment.to_dict())
            
            reviews_dict = {}
            for _, review in reviews_df.iterrows():
                order_id = review['order_id']
                if order_id not in reviews_dict:
                    reviews_dict[order_id] = review.to_dict()
            
            print(f"✅ Datos pre-procesados: {len(items_dict):,} órdenes con items, {len(payments_dict):,} con pagos, {len(reviews_dict):,} con reviews")
            
            # OPTIMIZACIÓN 3: Procesar en lotes más grandes y usar bulk operations
            batch_size = 5000  # Aumentar tamaño de lote
            documents = []
            processed = 0
            start_time = time.time()
            
            print(f"🚀 Iniciando procesamiento optimizado...")
            
            for _, order in orders_df.iterrows():
                order_id = order['order_id']
                
                # Obtener datos pre-procesados (acceso O(1))
                order_items = items_dict.get(order_id, [])
                order_payments = payments_dict.get(order_id, [])
                order_review = reviews_dict.get(order_id, None)
                
                # Crear documento de orden (simplificado)
                doc = {
                    'order_id': order_id,
                    'customer': {
                        'customer_id': order['customer_id'],
                        'customer_city': order['customer_city_normalized'],
                        'customer_state': order['customer_state_normalized'],
                        'customer_region': order['customer_region']
                    },
                    'order_info': {
                        'order_status': order['order_status'],
                        'delivery_status': order['delivery_status'],
                        'order_purchase_timestamp': pd.to_datetime(order['order_purchase_timestamp']),
                        'order_approved_at': pd.to_datetime(order['order_approved_at']) if pd.notna(order['order_approved_at']) else None,
                        'order_delivered_carrier_date': pd.to_datetime(order['order_delivered_carrier_date']) if pd.notna(order['order_delivered_carrier_date']) else None,
                        'order_delivered_customer_date': pd.to_datetime(order['order_delivered_customer_date']) if pd.notna(order['order_delivered_customer_date']) else None,
                        'order_estimated_delivery_date': pd.to_datetime(order['order_estimated_delivery_date']) if pd.notna(order['order_estimated_delivery_date']) else None,
                        'delivery_time_days': order['delivery_time_days']
                    },
                    'time_dimensions': {
                        'order_year': order['order_year'],
                        'order_month': order['order_month'],
                        'order_day': order['order_day'],
                        'order_weekday': order['order_weekday'],
                        'order_quarter': order['order_quarter']
                    },
                    'items': [],
                    'payments': [],
                    'review': {},
                    'order_summary': {
                        'total_items': len(order_items),
                        'total_value': sum(item['total_item_value'] for item in order_items) if order_items else 0,
                        'total_freight': sum(item['freight_value'] for item in order_items) if order_items else 0,
                        'payment_methods_count': len(order_payments),
                        'average_review_score': order_review['review_score'] if order_review else None
                    },
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                # Agregar items (optimizado)
                for item in order_items:
                    item_doc = {
                        'order_item_id': item['order_item_id'],
                        'product_id': item['product_id'],
                        'seller_id': item['seller_id'],
                        'product_info': {
                            'product_category_name': item['product_category_name'],
                            'product_category_name_normalized': item['product_category_name_normalized'],
                            'weight_category': item['weight_category'],
                            'size_category': item['size_category']
                        },
                        'price': item['price'],
                        'freight_value': item['freight_value'],
                        'total_item_value': item['total_item_value'],
                        'freight_percentage': item['freight_percentage'],
                        'value_category': item['value_category'],
                        'freight_category': item['freight_category'],
                        'shipping_limit_date': pd.to_datetime(item['shipping_limit_date']) if pd.notna(item['shipping_limit_date']) else None
                    }
                    doc['items'].append(self.clean_for_mongodb(item_doc))
                
                # Agregar pagos (optimizado)
                for payment in order_payments:
                    payment_doc = {
                        'payment_sequential': payment['payment_sequential'],
                        'payment_type': payment['payment_type'],
                        'payment_type_normalized': payment['payment_type_normalized'],
                        'payment_installments': payment['payment_installments'],
                        'payment_value': payment['payment_value'],
                        'payment_value_category': payment['payment_value_category'],
                        'installments_category': payment['installments_category']
                    }
                    doc['payments'].append(self.clean_for_mongodb(payment_doc))
                
                # Agregar review (optimizado)
                if order_review is not None:
                    doc['review'] = {
                        'review_id': order_review['review_id'],
                        'review_score': order_review['review_score'],
                        'review_score_category': order_review['review_score_category'],
                        'review_comment_title': order_review['review_comment_title'],
                        'review_comment_message': order_review['review_comment_message'],
                        'review_creation_date': pd.to_datetime(order_review['review_creation_date']) if pd.notna(order_review['review_creation_date']) else None,
                        'review_answer_timestamp': pd.to_datetime(order_review['review_answer_timestamp']) if pd.notna(order_review['review_answer_timestamp']) else None,
                        'has_comment_title': order_review['has_comment_title'],
                        'has_comment_message': order_review['has_comment_message'],
                        'response_time_hours': order_review['response_time_hours']
                    }
                
                doc = self.clean_for_mongodb(doc)
                documents.append(doc)
                
                processed += 1
                if processed % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed
                    print(f"📊 Procesadas {processed:,} órdenes... ({rate:.0f} órdenes/seg)")
            
            # OPTIMIZACIÓN 4: Insertar en lotes más grandes
            print(f"\n💾 Insertando {len(documents):,} documentos en MongoDB...")
            total_inserted = 0
            insert_start_time = time.time()
            
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                
                try:
                    result = collection.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    elapsed = time.time() - insert_start_time
                    rate = total_inserted / elapsed
                    print(f"✅ Lote {i//batch_size + 1}: {len(result.inserted_ids):,} órdenes insertadas ({rate:.0f} docs/seg)")
                    
                except BulkWriteError as e:
                    inserted = len(e.details['insertedIds'])
                    total_inserted += inserted
                    elapsed = time.time() - insert_start_time
                    rate = total_inserted / elapsed
                    print(f"⚠️ Lote {i//batch_size + 1}: {inserted:,} órdenes insertadas (algunos duplicados) ({rate:.0f} docs/seg)")
            
            total_time = time.time() - start_time
            final_rate = total_inserted / total_time
            
            print(f"✅ Total: {total_inserted:,} órdenes insertadas en {total_time:.1f}s ({final_rate:.0f} docs/seg)")
            
            self.load_report['collections_loaded']['orders'] = {
                'documents_inserted': total_inserted,
                'collection_size': collection.count_documents({}),
                'processing_time_seconds': total_time,
                'insertion_rate_docs_per_sec': final_rate
            }
    
    def create_additional_indexes(self):
        """Crear índices adicionales para optimizar consultas"""
        print("\n🔍 CREANDO ÍNDICES ADICIONALES...")
        print("="*60)
        
        # Crear índices básicos para orders (que se comentaron durante la carga)
        orders_collection = self.db['orders']
        
        print("📋 Creando índices básicos para orders...")
        orders_collection.create_index("order_id", unique=True)
        orders_collection.create_index("customer.customer_id")
        orders_collection.create_index("order_info.order_purchase_timestamp")
        orders_collection.create_index("customer.customer_state")
        orders_collection.create_index("customer.customer_region")
        orders_collection.create_index("order_info.order_status")
        orders_collection.create_index("time_dimensions.order_year")
        orders_collection.create_index("time_dimensions.order_month")
        orders_collection.create_index("order_summary.total_value")
        orders_collection.create_index("review.review_score")
        
        # Índices compuestos para consultas frecuentes
        print("🔗 Creando índices compuestos...")
        
        # Índice compuesto para consultas por cliente y fecha
        orders_collection.create_index([
            ("customer.customer_id", 1),
            ("order_info.order_purchase_timestamp", -1)
        ])
        
        # Índice compuesto para consultas por región y estado
        orders_collection.create_index([
            ("customer.customer_region", 1),
            ("customer.customer_state", 1)
        ])
        
        # Índice compuesto para consultas por estado de orden y fecha
        orders_collection.create_index([
            ("order_info.order_status", 1),
            ("order_info.order_purchase_timestamp", -1)
        ])
        
        # Índice compuesto para consultas por valor y fecha
        orders_collection.create_index([
            ("order_summary.total_value", -1),
            ("order_info.order_purchase_timestamp", -1)
        ])
        
        print("✅ Índices adicionales creados")
    
    def generate_collection_statistics(self):
        """Generar estadísticas de las colecciones"""
        print("\n📊 ESTADÍSTICAS DE COLECCIONES...")
        print("="*60)
        
        stats = {}
        
        for collection_name in ['orders', 'products', 'customers', 'sellers']:
            collection = self.db[collection_name]
            count = collection.count_documents({})
            
            # Obtener tamaño aproximado
            stats_command = self.db.command("collStats", collection_name)
            size_mb = stats_command['size'] / (1024 * 1024)
            
            stats[collection_name] = {
                'documents': count,
                'size_mb': round(size_mb, 2)
            }
            
            print(f"📁 {collection_name}: {count:,} documentos, {size_mb:.2f} MB")
        
        self.load_report['collection_statistics'] = stats
        self.load_report['total_documents'] = sum(stats[col]['documents'] for col in stats)
    
    def save_load_report(self):
        """Guardar reporte de carga"""
        print("\n💾 GUARDANDO REPORTE DE CARGA...")
        print("="*60)
        
        self.load_report['end_time'] = datetime.now().isoformat()
        self.load_report['total_loading_time'] = float((
            datetime.fromisoformat(self.load_report['end_time']) - 
            datetime.fromisoformat(self.load_report['start_time'])
        ).total_seconds())
        
        report_path = self.processed_data_path / 'mongodb_load_report.json'
        
        # Convertir datetime objects para JSON
        def datetime_converter(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.load_report, f, indent=2, ensure_ascii=False, default=datetime_converter)
        
        print(f"📋 Reporte guardado en: {report_path}")
    
    def print_final_summary(self):
        """Imprimir resumen final"""
        print("\n🎉 RESUMEN FINAL DE CARGA")
        print("="*60)
        
        total_docs = self.load_report['total_documents']
        total_time = self.load_report['total_loading_time']
        
        print(f"📊 Total documentos cargados: {total_docs:,}")
        print(f"⏱️ Tiempo total de carga: {total_time:.2f} segundos")
        print(f"🚀 Velocidad: {total_docs/total_time:.0f} documentos/segundo")
        
        print(f"\n📁 Colecciones cargadas:")
        for collection_name, stats in self.load_report['collection_statistics'].items():
            print(f"  • {collection_name}: {stats['documents']:,} documentos, {stats['size_mb']} MB")
        
        print(f"\n✅ CARGA A MONGODB COMPLETADA")
        print("📝 Próximos pasos:")
        print("   1. Verificar datos en MongoDB")
        print("   2. Implementar consultas CRUD")
        print("   3. Configurar replicación")
        print("   4. Probar consultas de rendimiento")
    
    def run_full_load(self):
        """Ejecutar proceso completo de carga"""
        print("🎯 CARGA DE DATOS A MONGODB")
        print("="*80)
        
        # Conectar a MongoDB
        self.connect_to_mongodb()
        
        # Limpiar colecciones existentes
        self.clean_existing_collections()
        
        # Cargar datasets procesados
        self.load_processed_datasets()
        
        # Cargar colecciones
        self.load_products_collection()
        self.load_customers_collection()
        self.load_sellers_collection()
        self.load_orders_collection()
        
        # Crear índices adicionales
        self.create_additional_indexes()
        
        # Generar estadísticas
        self.generate_collection_statistics()
        
        # Guardar reporte
        self.save_load_report()
        
        # Mostrar resumen
        self.print_final_summary()
        
        # Cerrar conexión
        if self.client:
            self.client.close()
            print("\n🔌 Conexión a MongoDB cerrada")

if __name__ == "__main__":
    loader = MongoDBDataLoader()
    loader.run_full_load()
