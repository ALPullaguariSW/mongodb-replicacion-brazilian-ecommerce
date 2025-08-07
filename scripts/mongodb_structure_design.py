#!/usr/bin/env python3
"""
Diseño de Estructura NoSQL para MongoDB
Dataset: Brazilian E-Commerce (datos procesados)
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class MongoDBStructureDesigner:
    def __init__(self, processed_data_path='data/processed'):
        self.processed_data_path = Path(processed_data_path)
        self.datasets = {}
        self.mongodb_structure = {}
        
    def load_processed_datasets(self):
        """Cargar datasets procesados"""
        print("📥 CARGANDO DATASETS PROCESADOS...")
        print("="*60)
        
        csv_files = list(self.processed_data_path.glob('*.csv'))
        
        for file_path in csv_files:
            if file_path.name != 'geolocation.csv':  # No necesitamos geolocation como colección separada
                filename = file_path.name.replace('.csv', '')
                print(f"📥 Cargando {filename}...")
                
                try:
                    df = pd.read_csv(file_path, low_memory=False)
                    self.datasets[filename] = df
                    print(f"✅ {filename}: {len(df):,} filas")
                    
                except Exception as e:
                    print(f"❌ Error cargando {filename}: {e}")
        
        print(f"📁 Total datasets cargados: {len(self.datasets)}")
    
    def design_orders_collection(self):
        """Diseñar colección principal de órdenes con datos anidados"""
        print("\n🗄️ DISEÑANDO COLECCIÓN ORDERS (PRINCIPAL)")
        print("="*60)
        
        if 'orders_with_customers' in self.datasets and 'items_with_products' in self.datasets:
            orders_df = self.datasets['orders_with_customers']
            items_df = self.datasets['items_with_products']
            payments_df = self.datasets['payments']
            reviews_df = self.datasets['reviews']
            
            # Estructura de la colección orders
            orders_structure = {
                'collection_name': 'orders',
                'description': 'Colección principal con órdenes y datos anidados',
                'document_structure': {
                    '_id': 'ObjectId (auto-generado)',
                    'order_id': 'String (único)',
                    'customer': {
                        'customer_id': 'String',
                        'customer_unique_id': 'String',
                        'customer_city': 'String',
                        'customer_state': 'String',
                        'customer_region': 'String',
                        'customer_zip_code_prefix': 'Number'
                    },
                    'order_info': {
                        'order_status': 'String',
                        'delivery_status': 'String',
                        'order_purchase_timestamp': 'Date',
                        'order_approved_at': 'Date',
                        'order_delivered_carrier_date': 'Date',
                        'order_delivered_customer_date': 'Date',
                        'order_estimated_delivery_date': 'Date',
                        'delivery_time_days': 'Number'
                    },
                    'time_dimensions': {
                        'order_year': 'Number',
                        'order_month': 'Number',
                        'order_day': 'Number',
                        'order_weekday': 'String',
                        'order_quarter': 'Number'
                    },
                    'items': [
                        {
                            'order_item_id': 'Number',
                            'product_id': 'String',
                            'seller_id': 'String',
                            'product_info': {
                                'product_category_name': 'String',
                                'product_category_name_normalized': 'String',
                                'weight_category': 'String',
                                'size_category': 'String'
                            },
                            'price': 'Number',
                            'freight_value': 'Number',
                            'total_item_value': 'Number',
                            'freight_percentage': 'Number',
                            'value_category': 'String',
                            'freight_category': 'String',
                            'shipping_limit_date': 'Date'
                        }
                    ],
                    'payments': [
                        {
                            'payment_sequential': 'Number',
                            'payment_type': 'String',
                            'payment_type_normalized': 'String',
                            'payment_installments': 'Number',
                            'payment_value': 'Number',
                            'payment_value_category': 'String',
                            'installments_category': 'String'
                        }
                    ],
                    'review': {
                        'review_id': 'String',
                        'review_score': 'Number',
                        'review_score_category': 'String',
                        'review_comment_title': 'String',
                        'review_comment_message': 'String',
                        'review_creation_date': 'Date',
                        'review_answer_timestamp': 'Date',
                        'has_comment_title': 'Boolean',
                        'has_comment_message': 'Boolean',
                        'response_time_hours': 'Number'
                    },
                    'order_summary': {
                        'total_items': 'Number',
                        'total_value': 'Number',
                        'total_freight': 'Number',
                        'payment_methods_count': 'Number',
                        'average_review_score': 'Number'
                    },
                    'created_at': 'Date',
                    'updated_at': 'Date'
                },
                'estimated_documents': len(orders_df),
                'estimated_size_mb': len(orders_df) * 0.5  # Estimación aproximada
            }
            
            self.mongodb_structure['orders'] = orders_structure
            print(f"✅ Estructura de orders diseñada para {len(orders_df):,} documentos")
            
            return orders_df, items_df, payments_df, reviews_df
    
    def design_products_collection(self):
        """Diseñar colección de productos"""
        print("\n🗄️ DISEÑANDO COLECCIÓN PRODUCTS")
        print("="*60)
        
        if 'products' in self.datasets:
            products_df = self.datasets['products']
            
            products_structure = {
                'collection_name': 'products',
                'description': 'Colección de productos con información detallada',
                'document_structure': {
                    '_id': 'ObjectId (auto-generado)',
                    'product_id': 'String (único)',
                    'product_category_name': 'String',
                    'product_category_name_normalized': 'String',
                    'product_name_lenght': 'Number',
                    'product_description_lenght': 'Number',
                    'product_photos_qty': 'Number',
                    'product_weight_g': 'Number',
                    'product_length_cm': 'Number',
                    'product_height_cm': 'Number',
                    'product_width_cm': 'Number',
                    'product_volume_cm3': 'Number',
                    'weight_category': 'String',
                    'size_category': 'String',
                    'created_at': 'Date',
                    'updated_at': 'Date'
                },
                'estimated_documents': len(products_df),
                'estimated_size_mb': len(products_df) * 0.2
            }
            
            self.mongodb_structure['products'] = products_structure
            print(f"✅ Estructura de products diseñada para {len(products_df):,} documentos")
    
    def design_customers_collection(self):
        """Diseñar colección de clientes"""
        print("\n🗄️ DISEÑANDO COLECCIÓN CUSTOMERS")
        print("="*60)
        
        if 'customers' in self.datasets:
            customers_df = self.datasets['customers']
            
            customers_structure = {
                'collection_name': 'customers',
                'description': 'Colección de clientes con información geográfica',
                'document_structure': {
                    '_id': 'ObjectId (auto-generado)',
                    'customer_id': 'String (único)',
                    'customer_unique_id': 'String',
                    'customer_zip_code_prefix': 'Number',
                    'customer_city': 'String',
                    'customer_state': 'String',
                    'customer_city_normalized': 'String',
                    'customer_state_normalized': 'String',
                    'customer_region': 'String',
                    'created_at': 'Date',
                    'updated_at': 'Date'
                },
                'estimated_documents': len(customers_df),
                'estimated_size_mb': len(customers_df) * 0.1
            }
            
            self.mongodb_structure['customers'] = customers_structure
            print(f"✅ Estructura de customers diseñada para {len(customers_df):,} documentos")
    
    def design_sellers_collection(self):
        """Diseñar colección de vendedores"""
        print("\n🗄️ DISEÑANDO COLECCIÓN SELLERS")
        print("="*60)
        
        if 'sellers' in self.datasets:
            sellers_df = self.datasets['sellers']
            
            sellers_structure = {
                'collection_name': 'sellers',
                'description': 'Colección de vendedores',
                'document_structure': {
                    '_id': 'ObjectId (auto-generado)',
                    'seller_id': 'String (único)',
                    'seller_zip_code_prefix': 'Number',
                    'seller_city': 'String',
                    'seller_state': 'String',
                    'created_at': 'Date',
                    'updated_at': 'Date'
                },
                'estimated_documents': len(sellers_df),
                'estimated_size_mb': len(sellers_df) * 0.05
            }
            
            self.mongodb_structure['sellers'] = sellers_structure
            print(f"✅ Estructura de sellers diseñada para {len(sellers_df):,} documentos")
    
    def design_indexes(self):
        """Diseñar índices para optimizar consultas"""
        print("\n🔍 DISEÑANDO ÍNDICES")
        print("="*60)
        
        indexes = {
            'orders': [
                {'field': 'order_id', 'type': 'unique'},
                {'field': 'customer.customer_id', 'type': 'index'},
                {'field': 'order_info.order_purchase_timestamp', 'type': 'index'},
                {'field': 'customer.customer_state', 'type': 'index'},
                {'field': 'customer.customer_region', 'type': 'index'},
                {'field': 'order_info.order_status', 'type': 'index'},
                {'field': 'time_dimensions.order_year', 'type': 'index'},
                {'field': 'time_dimensions.order_month', 'type': 'index'},
                {'field': 'order_summary.total_value', 'type': 'index'},
                {'field': 'review.review_score', 'type': 'index'}
            ],
            'products': [
                {'field': 'product_id', 'type': 'unique'},
                {'field': 'product_category_name', 'type': 'index'},
                {'field': 'weight_category', 'type': 'index'},
                {'field': 'size_category', 'type': 'index'}
            ],
            'customers': [
                {'field': 'customer_id', 'type': 'unique'},
                {'field': 'customer_state', 'type': 'index'},
                {'field': 'customer_region', 'type': 'index'},
                {'field': 'customer_city', 'type': 'index'}
            ],
            'sellers': [
                {'field': 'seller_id', 'type': 'unique'},
                {'field': 'seller_state', 'type': 'index'},
                {'field': 'seller_city', 'type': 'index'}
            ]
        }
        
        self.mongodb_structure['indexes'] = indexes
        
        total_indexes = sum(len(index_list) for index_list in indexes.values())
        print(f"✅ {total_indexes} índices diseñados para optimizar consultas")
    
    def generate_sample_documents(self):
        """Generar ejemplos de documentos MongoDB"""
        print("\n📄 GENERANDO EJEMPLOS DE DOCUMENTOS")
        print("="*60)
        
        if 'orders_with_customers' in self.datasets:
            orders_df = self.datasets['orders_with_customers']
            items_df = self.datasets['items_with_products']
            payments_df = self.datasets['payments']
            reviews_df = self.datasets['reviews']
            
            # Tomar una orden de ejemplo
            sample_order = orders_df.iloc[0]
            order_id = sample_order['order_id']
            
            # Obtener items de esta orden
            order_items = items_df[items_df['order_id'] == order_id]
            order_payments = payments_df[payments_df['order_id'] == order_id]
            order_review = reviews_df[reviews_df['order_id'] == order_id]
            
            # Crear documento de ejemplo
            sample_document = {
                'order_id': order_id,
                'customer': {
                    'customer_id': sample_order['customer_id'],
                    'customer_city': sample_order['customer_city_normalized'],
                    'customer_state': sample_order['customer_state_normalized'],
                    'customer_region': sample_order['customer_region']
                },
                'order_info': {
                    'order_status': sample_order['order_status'],
                    'delivery_status': sample_order['delivery_status'],
                    'order_purchase_timestamp': sample_order['order_purchase_timestamp'],
                    'delivery_time_days': sample_order['delivery_time_days']
                },
                'items': order_items[['order_item_id', 'product_id', 'price', 'freight_value', 
                                    'total_item_value', 'product_category_name']].to_dict('records'),
                'payments': order_payments[['payment_type_normalized', 'payment_value', 
                                          'payment_installments']].to_dict('records'),
                'review': {
                    'review_score': order_review['review_score'].iloc[0] if len(order_review) > 0 else None,
                    'review_score_category': order_review['review_score_category'].iloc[0] if len(order_review) > 0 else None
                },
                'order_summary': {
                    'total_items': len(order_items),
                    'total_value': order_items['total_item_value'].sum(),
                    'total_freight': order_items['freight_value'].sum()
                }
            }
            
            self.mongodb_structure['sample_document'] = sample_document
            print("✅ Documento de ejemplo generado")
    
    def save_mongodb_design(self):
        """Guardar diseño de estructura MongoDB"""
        print("\n💾 GUARDANDO DISEÑO MONGODB")
        print("="*60)
        
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
        
        clean_structure = clean_for_json(self.mongodb_structure)
        
        design_path = self.processed_data_path / 'mongodb_structure_design.json'
        
        with open(design_path, 'w', encoding='utf-8') as f:
            json.dump(clean_structure, f, indent=2, ensure_ascii=False)
        
        print(f"📋 Diseño guardado en: {design_path}")
    
    def print_design_summary(self):
        """Imprimir resumen del diseño"""
        print("\n📊 RESUMEN DEL DISEÑO MONGODB")
        print("="*60)
        
        total_documents = 0
        total_size_mb = 0
        
        for collection_name, structure in self.mongodb_structure.items():
            if collection_name not in ['indexes', 'sample_document']:
                docs = structure['estimated_documents']
                size = structure['estimated_size_mb']
                total_documents += docs
                total_size_mb += size
                
                print(f"📁 {collection_name}: {docs:,} documentos, {size:.2f} MB")
        
        print(f"\n📊 TOTAL:")
        print(f"  • Colecciones: {len(self.mongodb_structure) - 2}")
        print(f"  • Documentos: {total_documents:,}")
        print(f"  • Tamaño estimado: {total_size_mb:.2f} MB")
        
        print(f"\n🎯 VENTAJAS DEL DISEÑO NOSQL:")
        print(f"  • Datos anidados: Información completa en un documento")
        print(f"  • Consultas eficientes: Menos joins necesarios")
        print(f"  • Escalabilidad: Fácil particionamiento")
        print(f"  • Flexibilidad: Estructura adaptable")
    
    def run_design_process(self):
        """Ejecutar proceso completo de diseño"""
        print("🎯 DISEÑO DE ESTRUCTURA NOSQL PARA MONGODB")
        print("="*80)
        
        # Cargar datos procesados
        self.load_processed_datasets()
        
        # Diseñar colecciones
        self.design_orders_collection()
        self.design_products_collection()
        self.design_customers_collection()
        self.design_sellers_collection()
        
        # Diseñar índices
        self.design_indexes()
        
        # Generar ejemplos
        self.generate_sample_documents()
        
        # Guardar diseño
        self.save_mongodb_design()
        
        # Mostrar resumen
        self.print_design_summary()
        
        print("\n✅ DISEÑO MONGODB COMPLETADO")
        print("📝 Próximos pasos:")
        print("   1. Revisar estructura en mongodb_structure_design.json")
        print("   2. Crear script de carga a MongoDB")
        print("   3. Implementar consultas CRUD")
        print("   4. Configurar replicación")

if __name__ == "__main__":
    designer = MongoDBStructureDesigner()
    designer.run_design_process()
