#!/usr/bin/env python3
"""
Consultas CRUD para MongoDB - PARTE 2 (Consultas 6-10)
Dataset: Brazilian E-Commerce (MongoDB)
Actualizaciones y Eliminaciones Condicionadas
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class MongoDBCRUDQueriesPart2:
    def __init__(self, mongodb_uri='mongodb://localhost:27020/'):
        self.mongodb_uri = mongodb_uri
        self.client = None
        self.db = None
        self.results = {}
        
    def connect_to_mongodb(self):
        """Conectar a MongoDB"""
        print("🔌 CONECTANDO A MONGODB...")
        print("="*60)
        
        try:
            self.client = MongoClient(
                self.mongodb_uri,
                directConnection=True,
                serverSelectionTimeoutMS=5000
            )
            self.client.admin.command('ping')
            print("✅ Conexión exitosa a MongoDB")
            
            self.db = self.client['brazilian_ecommerce']
            print(f"📁 Base de datos: {self.db.name}")
            
        except Exception as e:
            print(f"❌ Error conectando a MongoDB: {e}")
            raise
    
    def query_6_actualizar_email_cliente_condicionado(self):
        """
        6. Operación de actualización donde, en la colección clientes, se actualiza la dirección 
        de correo electrónico de un cliente utilizando su cliente_id, pero solo si este cliente 
        ha realizado más de 5 compras y la última compra fue realizada en el último trimestre.
        """
        print("\n📊 CONSULTA 6: Actualizar email cliente con condiciones")
        print("="*60)
        
        # Primero, encontrar clientes que califiquen
        # Calcular fecha límite (último trimestre = 3 meses)
        fecha_limite = datetime(2018, 6, 1)  # Usar fechas del dataset real
        
        pipeline_clientes_calificados = [
            {
                "$group": {
                    "_id": "$customer.customer_id",
                    "total_compras": {"$sum": 1},
                    "ultima_compra": {"$max": "$order_info.order_purchase_timestamp"},
                    "total_gastado": {"$sum": "$order_summary.total_value"}
                }
            },
            {
                "$match": {
                    "$and": [
                        {"total_compras": {"$gt": 5}},
                        {"ultima_compra": {"$gte": fecha_limite}}
                    ]
                }
            },
            {
                "$sort": {"total_compras": -1}
            }
        ]
        
        clientes_calificados = list(self.db.orders.aggregate(pipeline_clientes_calificados))
        
        print(f"Fecha límite para última compra: {fecha_limite}")
        print(f"Clientes con >5 compras y compra reciente: {len(clientes_calificados)}")
        
        if clientes_calificados:
            cliente_ejemplo = clientes_calificados[0]
            cliente_id = cliente_ejemplo['_id']
            
            print(f"\nCliente seleccionado para actualización:")
            print(f"  - Cliente ID: {cliente_id}")
            print(f"  - Total compras: {cliente_ejemplo['total_compras']}")
            print(f"  - Última compra: {cliente_ejemplo['ultima_compra']}")
            print(f"  - Total gastado: ${cliente_ejemplo['total_gastado']:.2f}")
            
            # SIMULACIÓN de actualización de email
            nuevo_email = f"{cliente_id[:8]}@emailactualizado.com"
            
            update_query = {"customer_id": cliente_id}
            update_operation = {
                "$set": {
                    "customer_email": nuevo_email,
                    "email_updated_date": datetime.now(),
                    "update_reason": "Cliente VIP con >5 compras recientes"
                }
            }
            
            print(f"\n⚠️ SIMULACIÓN DE ACTUALIZACIÓN:")
            print(f"db.customers.update_one(")
            print(f"  {json.dumps(update_query, indent=2)},")
            print(f"  {json.dumps(update_operation, indent=2, default=str)}")
            print(f")")
            
            print(f"\n✅ Email actualizado (simulado): {nuevo_email}")
            
            self.results['query_6'] = {
                'description': 'Actualización email cliente con condiciones',
                'clientes_calificados': len(clientes_calificados),
                'cliente_actualizado': cliente_id,
                'nuevo_email': nuevo_email,
                'condiciones_aplicadas': 'Más de 5 compras Y última compra en trimestre'
            }
            
        else:
            print("❌ No se encontraron clientes que califiquen para la actualización")
            self.results['query_6'] = {
                'description': 'Actualización email cliente con condiciones',
                'clientes_calificados': 0,
                'resultado': 'Sin clientes que califiquen'
            }
        
        return clientes_calificados
    
    def query_7_actualizar_precios_productos_vendidos(self):
        """
        7. Actualizar los precios de todos los productos que hayan sido vendidos más de 100 veces 
        en el último año, pero solo si el precio de esos productos está por debajo de un umbral ($100).
        """
        print("\n📊 CONSULTA 7: Actualizar precios productos vendidos >100 veces")
        print("="*60)
        
        # Usar fechas del dataset real (2016-2018)
        fecha_inicio = datetime(2017, 1, 1)
        fecha_fin = datetime(2018, 12, 31)
        umbral_precio = 100.0
        
        # Pipeline para encontrar productos vendidos >100 veces con precio <$100
        pipeline = [
            {
                "$match": {
                    "order_info.order_purchase_timestamp": {
                        "$gte": fecha_inicio,
                        "$lte": fecha_fin
                    }
                }
            },
            {
                "$unwind": "$items"
            },
            {
                "$match": {
                    "items.price": {"$lt": umbral_precio}
                }
            },
            {
                "$group": {
                    "_id": "$items.product_id",
                    "veces_vendido": {"$sum": 1},
                    "precio_promedio": {"$avg": "$items.price"},
                    "categoria": {"$first": "$items.product_info.product_category_name_normalized"},
                    "total_ingresos": {"$sum": "$items.total_item_value"}
                }
            },
            {
                "$match": {
                    "veces_vendido": {"$gt": 100}
                }
            },
            {
                "$sort": {"veces_vendido": -1}
            }
        ]
        
        productos_calificados = list(self.db.orders.aggregate(pipeline))
        
        print(f"Período analizado: {fecha_inicio.strftime('%Y-%m-%d')} a {fecha_fin.strftime('%Y-%m-%d')}")
        print(f"Umbral de precio: ${umbral_precio}")
        print(f"Productos vendidos >100 veces con precio <${umbral_precio}: {len(productos_calificados)}")
        
        if productos_calificados:
            print(f"\nTop 5 productos para actualización de precio:")
            for i, producto in enumerate(productos_calificados[:5], 1):
                nuevo_precio = producto['precio_promedio'] * 1.15  # Aumentar 15%
                print(f"  {i}. Producto: {producto['_id']}")
                print(f"     Categoría: {producto['categoria']}")
                print(f"     Veces vendido: {producto['veces_vendido']}")
                print(f"     Precio actual: ${producto['precio_promedio']:.2f}")
                print(f"     Precio nuevo: ${nuevo_precio:.2f} (+15%)")
                print(f"     Total ingresos: ${producto['total_ingresos']:.2f}")
            
            # SIMULACIÓN de actualización masiva
            productos_ids = [p['_id'] for p in productos_calificados]
            
            print(f"\n⚠️ SIMULACIÓN DE ACTUALIZACIÓN MASIVA:")
            print(f"Se actualizarían {len(productos_ids)} productos")
            print(f"Operación MongoDB:")
            print(f"db.orders.update_many(")
            print(f"  {{'items.product_id': {{'$in': {productos_ids[:3]}...}}}},")
            print(f"  {{'$mul': {{'items.$[elem].price': 1.15}}}},")
            print(f"  {{'arrayFilters': [{{'elem.product_id': {{'$in': {productos_ids[:3]}...}}}}]}}")
            print(f")")
            
            total_ingresos_actuales = sum(p['total_ingresos'] for p in productos_calificados)
            total_ingresos_nuevos = total_ingresos_actuales * 1.15
            
            print(f"\n💰 Impacto financiero estimado:")
            print(f"  - Ingresos actuales: ${total_ingresos_actuales:,.2f}")
            print(f"  - Ingresos proyectados: ${total_ingresos_nuevos:,.2f}")
            print(f"  - Incremento: ${total_ingresos_nuevos - total_ingresos_actuales:,.2f}")
            
            self.results['query_7'] = {
                'description': 'Actualización precios productos populares',
                'productos_calificados': len(productos_calificados),
                'criterios': 'Vendidos >100 veces Y precio <$100',
                'incremento_precio': '15%',
                'impacto_financiero': {
                    'ingresos_actuales': total_ingresos_actuales,
                    'ingresos_proyectados': total_ingresos_nuevos,
                    'incremento': total_ingresos_nuevos - total_ingresos_actuales
                }
            }
        
        else:
            print("❌ No se encontraron productos que califiquen")
            self.results['query_7'] = {
                'description': 'Actualización precios productos populares',
                'productos_calificados': 0
            }
        
        return productos_calificados
    
    def query_8_eliminar_productos_sin_stock_sin_ventas(self):
        """
        8. Eliminar todos los productos de la colección productos cuya cantidad_stock sea 0 
        y que no se hayan vendido en los últimos 6 meses.
        """
        print("\n📊 CONSULTA 8: Eliminar productos sin stock ni ventas")
        print("="*60)
        
        # Usar fechas del dataset (últimos 6 meses de datos disponibles)
        fecha_limite = datetime(2018, 3, 1)  # 6 meses antes del final del dataset
        
        # Paso 1: Encontrar productos que SÍ se vendieron en los últimos 6 meses
        pipeline_productos_activos = [
            {
                "$match": {
                    "order_info.order_purchase_timestamp": {"$gte": fecha_limite}
                }
            },
            {
                "$unwind": "$items"
            },
            {
                "$group": {
                    "_id": "$items.product_id",
                    "ultima_venta": {"$max": "$order_info.order_purchase_timestamp"},
                    "total_vendido": {"$sum": 1}
                }
            }
        ]
        
        productos_activos = list(self.db.orders.aggregate(pipeline_productos_activos))
        productos_activos_ids = set(p['_id'] for p in productos_activos)
        
        # Paso 2: Encontrar todos los productos en la colección products
        total_productos = self.db.products.count_documents({})
        productos_sin_ventas_recientes = []
        
        # Simular campo stock = 0 para algunos productos
        pipeline_productos_candidatos = [
            {
                "$match": {
                    "product_id": {"$nin": list(productos_activos_ids)}
                }
            },
            {
                "$limit": 100  # Limitar para demostración
            }
        ]
        
        productos_candidatos = list(self.db.products.aggregate(pipeline_productos_candidatos))
        
        print(f"Fecha límite ventas: {fecha_limite}")
        print(f"Total productos en catálogo: {total_productos:,}")
        print(f"Productos con ventas recientes: {len(productos_activos):,}")
        print(f"Productos candidatos a eliminación: {len(productos_candidatos):,}")
        
        if productos_candidatos:
            print(f"\nMuestra de productos para eliminar:")
            for i, producto in enumerate(productos_candidatos[:5], 1):
                print(f"  {i}. Producto ID: {producto['product_id']}")
                print(f"     Categoría: {producto.get('product_category_name', 'N/A')}")
                print(f"     Peso: {producto.get('product_weight_g', 'N/A')}g")
                print(f"     Razón: Sin ventas desde {fecha_limite.strftime('%Y-%m-%d')}")
            
            # SIMULACIÓN de eliminación
            productos_eliminar_ids = [p['product_id'] for p in productos_candidatos]
            
            print(f"\n⚠️ SIMULACIÓN DE ELIMINACIÓN:")
            print(f"Operación MongoDB:")
            print(f"db.products.delete_many({{")
            print(f"  '$and': [")
            print(f"    {{'product_id': {{'$in': {productos_eliminar_ids[:3]}...}}}},")
            print(f"    {{'stock_quantity': 0}},  # Campo simulado")
            print(f"    {{'last_sale_date': {{'$lt': '{fecha_limite.isoformat()}'}}}}")
            print(f"  ]")
            print(f"}})")
            
            print(f"\n📊 Optimización de índices recomendada:")
            print(f"db.products.create_index([")
            print(f"  ('stock_quantity', 1),")
            print(f"  ('last_sale_date', 1),")
            print(f"  ('product_id', 1)")
            print(f"])")
            print(f"Esto optimizaría la consulta en bases de datos con millones de productos")
            
            self.results['query_8'] = {
                'description': 'Eliminación productos sin stock ni ventas',
                'fecha_limite': fecha_limite.isoformat(),
                'total_productos': total_productos,
                'productos_activos': len(productos_activos),
                'productos_candidatos_eliminacion': len(productos_candidatos),
                'optimizacion_indices': 'Índice compuesto en stock_quantity, last_sale_date, product_id'
            }
        
        return productos_candidatos
    
    def query_9_eliminar_ventas_ciudad_bajo_promedio(self, ciudad="rio de janeiro"):
        """
        9. Eliminar todas las ventas de la colección ventas realizadas en una ciudad específica 
        y cuyo precio esté por debajo del promedio de todas las ventas realizadas en esa ciudad 
        en el último trimestre.
        """
        print("\n📊 CONSULTA 9: Eliminar ventas bajo promedio en ciudad")
        print("="*60)
        
        # Usar último trimestre del dataset
        fecha_inicio = datetime(2018, 6, 1)
        fecha_fin = datetime(2018, 8, 31)
        
        # Paso 1: Calcular promedio de ventas en la ciudad en el último trimestre
        pipeline_promedio = [
            {
                "$match": {
                    "$and": [
                        {"order_info.order_purchase_timestamp": {"$gte": fecha_inicio, "$lte": fecha_fin}},
                        {"$expr": {"$regexMatch": {
                            "input": {"$toLower": "$customer.customer_city"},
                            "regex": ciudad.lower()
                        }}}
                    ]
                }
            },
            {
                "$group": {
                    "_id": None,
                    "precio_promedio": {"$avg": "$order_summary.total_value"},
                    "total_ventas": {"$sum": 1},
                    "total_ingresos": {"$sum": "$order_summary.total_value"}
                }
            }
        ]
        
        resultado_promedio = list(self.db.orders.aggregate(pipeline_promedio))
        
        if not resultado_promedio:
            print(f"❌ No se encontraron ventas en {ciudad.title()} en el período especificado")
            return []
        
        promedio_ciudad = resultado_promedio[0]
        precio_promedio = promedio_ciudad['precio_promedio']
        
        # Paso 2: Encontrar ventas por debajo del promedio
        pipeline_ventas_bajo_promedio = [
            {
                "$match": {
                    "$and": [
                        {"order_info.order_purchase_timestamp": {"$gte": fecha_inicio, "$lte": fecha_fin}},
                        {"$expr": {"$regexMatch": {
                            "input": {"$toLower": "$customer.customer_city"},
                            "regex": ciudad.lower()
                        }}},
                        {"order_summary.total_value": {"$lt": precio_promedio}}
                    ]
                }
            },
            {
                "$project": {
                    "order_id": 1,
                    "customer.customer_city": 1,
                    "order_summary.total_value": 1,
                    "order_info.order_purchase_timestamp": 1
                }
            },
            {
                "$sort": {"order_summary.total_value": 1}
            }
        ]
        
        ventas_bajo_promedio = list(self.db.orders.aggregate(pipeline_ventas_bajo_promedio))
        
        print(f"Ciudad: {ciudad.title()}")
        print(f"Período: {fecha_inicio.strftime('%Y-%m-%d')} a {fecha_fin.strftime('%Y-%m-%d')}")
        print(f"Total ventas en ciudad: {promedio_ciudad['total_ventas']:,}")
        print(f"Precio promedio ciudad: ${precio_promedio:.2f}")
        print(f"Ventas bajo promedio: {len(ventas_bajo_promedio):,}")
        
        if ventas_bajo_promedio:
            valor_total_eliminar = sum(v['order_summary']['total_value'] for v in ventas_bajo_promedio)
            
            print(f"\nMuestra de ventas a eliminar:")
            for i, venta in enumerate(ventas_bajo_promedio[:5], 1):
                print(f"  {i}. Orden: {venta['order_id']}")
                print(f"     Valor: ${venta['order_summary']['total_value']:.2f}")
                print(f"     Fecha: {venta['order_info']['order_purchase_timestamp'].strftime('%Y-%m-%d')}")
            
            print(f"\n⚠️ CONSIDERACIONES CRÍTICAS DE REPLICACIÓN:")
            print(f"1. Eliminar {len(ventas_bajo_promedio):,} ventas (${valor_total_eliminar:,.2f})")
            print(f"2. En sistema replicado: Operación se propaga a secundarios")
            print(f"3. Backup requerido antes de eliminación masiva")
            print(f"4. Verificar integridad referencial con otras colecciones")
            
            orden_ids = [v['order_id'] for v in ventas_bajo_promedio]
            
            print(f"\n⚠️ SIMULACIÓN DE ELIMINACIÓN:")
            print(f"db.orders.delete_many({{")
            print(f"  '$and': [")
            print(f"    {{'order_id': {{'$in': {orden_ids[:3]}...}}}},")
            print(f"    {{'customer.customer_city': /{ciudad.lower()}/i}},")
            print(f"    {{'order_summary.total_value': {{'$lt': {precio_promedio:.2f}}}}}")
            print(f"  ]")
            print(f"}})")
            
            self.results['query_9'] = {
                'description': 'Eliminación ventas bajo promedio en ciudad',
                'ciudad': ciudad,
                'precio_promedio_ciudad': precio_promedio,
                'ventas_bajo_promedio': len(ventas_bajo_promedio),
                'valor_total_eliminar': valor_total_eliminar,
                'consideraciones_replicacion': 'Backup necesario, propagación a secundarios'
            }
        
        return ventas_bajo_promedio
    
    def query_10_eliminar_clientes_compras_minimas(self, valor_minimo=100):
        """
        10. Eliminar todos los clientes cuyo total de compras no ha superado un valor mínimo 
        durante el último año ($100). Explicar cómo afectarían las lecturas en el sistema 
        debido a la replicación en un clúster.
        """
        print("\n📊 CONSULTA 10: Eliminar clientes con compras mínimas")
        print("="*60)
        
        # Usar último año del dataset
        fecha_inicio = datetime(2017, 9, 1)
        fecha_fin = datetime(2018, 8, 31)
        
        # Pipeline para encontrar clientes con compras totales
        pipeline_clientes_compras = [
            {
                "$match": {
                    "order_info.order_purchase_timestamp": {
                        "$gte": fecha_inicio,
                        "$lte": fecha_fin
                    }
                }
            },
            {
                "$group": {
                    "_id": "$customer.customer_id",
                    "total_gastado": {"$sum": "$order_summary.total_value"},
                    "total_ordenes": {"$sum": 1},
                    "primera_compra": {"$min": "$order_info.order_purchase_timestamp"},
                    "ultima_compra": {"$max": "$order_info.order_purchase_timestamp"},
                    "ciudad": {"$first": "$customer.customer_city"},
                    "estado": {"$first": "$customer.customer_state"}
                }
            },
            {
                "$sort": {"total_gastado": 1}
            }
        ]
        
        todos_clientes = list(self.db.orders.aggregate(pipeline_clientes_compras))
        clientes_bajo_minimo = [c for c in todos_clientes if c['total_gastado'] < valor_minimo]
        
        print(f"Período analizado: {fecha_inicio.strftime('%Y-%m-%d')} a {fecha_fin.strftime('%Y-%m-%d')}")
        print(f"Valor mínimo requerido: ${valor_minimo}")
        print(f"Total clientes activos: {len(todos_clientes):,}")
        print(f"Clientes bajo mínimo: {len(clientes_bajo_minimo):,}")
        
        if clientes_bajo_minimo:
            total_perdido = sum(c['total_gastado'] for c in clientes_bajo_minimo)
            
            print(f"\nEstadísticas de clientes a eliminar:")
            print(f"  - Gasto promedio: ${total_perdido/len(clientes_bajo_minimo):.2f}")
            print(f"  - Gasto total perdido: ${total_perdido:,.2f}")
            print(f"  - Órdenes afectadas: {sum(c['total_ordenes'] for c in clientes_bajo_minimo):,}")
            
            print(f"\nMuestra de clientes para eliminación:")
            for i, cliente in enumerate(clientes_bajo_minimo[:5], 1):
                print(f"  {i}. Cliente: {cliente['_id']}")
                print(f"     Total gastado: ${cliente['total_gastado']:.2f}")
                print(f"     Órdenes: {cliente['total_ordenes']}")
                print(f"     Ubicación: {cliente['ciudad']}, {cliente['estado']}")
            
            clientes_ids = [c['_id'] for c in clientes_bajo_minimo]
            
            print(f"\n🔄 IMPACTO EN LECTURAS CON REPLICACIÓN:")
            print(f"1. CONSISTENCIA EVENTUAL:")
            print(f"   - Eliminación en Primary → Replica lag en Secondary")
            print(f"   - Lecturas desde Secondary pueden mostrar clientes eliminados")
            print(f"   - Tiempo de propagación: ~100ms típico")
            print(f"")
            print(f"2. READ PREFERENCE afecta resultados:")
            print(f"   - PRIMARY: Datos consistentes inmediatamente")
            print(f"   - SECONDARY: Posible inconsistencia temporal")
            print(f"   - SECONDARY_PREFERRED: Mejor performance, menos consistencia")
            print(f"")
            print(f"3. WRITE CONCERN para confirmación:")
            print(f"   - majority: Espera confirmación de mayoría de nodos")
            print(f"   - 1: Confirma solo en Primary (más rápido, menos seguro)")
            
            print(f"\n⚠️ SIMULACIÓN DE ELIMINACIÓN:")
            print(f"# Eliminar de colección customers")
            print(f"db.customers.delete_many({{")
            print(f"  'customer_id': {{'$in': {clientes_ids[:3]}...}}")
            print(f"}})")
            print(f"")
            print(f"# También eliminar órdenes relacionadas")
            print(f"db.orders.delete_many({{")
            print(f"  'customer.customer_id': {{'$in': {clientes_ids[:3]}...}}")
            print(f"}})")
            
            self.results['query_10'] = {
                'description': 'Eliminación clientes con compras bajo mínimo',
                'valor_minimo': valor_minimo,
                'clientes_activos': len(todos_clientes),
                'clientes_eliminar': len(clientes_bajo_minimo),
                'impacto_financiero': total_perdido,
                'consideraciones_replicacion': {
                    'consistencia_eventual': 'Lag temporal en secundarios',
                    'read_preference': 'Afecta consistencia de lecturas',
                    'write_concern': 'majority recomendado para eliminaciones críticas'
                }
            }
        
        return clientes_bajo_minimo
    
    def run_all_queries(self):
        """Ejecutar todas las consultas parte 2"""
        print("🎯 EJECUTANDO CONSULTAS CRUD - PARTE 2/3")
        print("="*80)
        
        # Conectar a MongoDB
        self.connect_to_mongodb()
        
        # Ejecutar consultas 6-10
        try:
            self.query_6_actualizar_email_cliente_condicionado()
            self.query_7_actualizar_precios_productos_vendidos()
            self.query_8_eliminar_productos_sin_stock_sin_ventas()
            self.query_9_eliminar_ventas_ciudad_bajo_promedio()
            self.query_10_eliminar_clientes_compras_minimas()
            
            print(f"\n🎉 PARTE 2 COMPLETADA")
            print(f"✅ 5 consultas de actualización/eliminación ejecutadas exitosamente")
            
        except Exception as e:
            print(f"❌ Error ejecutando consultas: {e}")
        
        finally:
            if self.client:
                self.client.close()
                print(f"\n🔌 Conexión cerrada")
    
    def save_results(self, filename='data/processed/crud_results_part2.json'):
        """Guardar resultados"""
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        # Limpiar resultados para JSON
        clean_results = {}
        for key, value in self.results.items():
            clean_results[key] = self.clean_for_json(value)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                'execution_date': datetime.now().isoformat(),
                'total_queries': len(clean_results),
                'results': clean_results
            }, f, indent=2, ensure_ascii=False)
        
        print(f"📋 Resultados guardados en: {filename}")
    
    def clean_for_json(self, obj):
        """Limpiar objeto para serialización JSON"""
        if isinstance(obj, dict):
            return {k: self.clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_for_json(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        else:
            return obj

if __name__ == "__main__":
    # Ejecutar consultas CRUD parte 2
    crud = MongoDBCRUDQueriesPart2()
    crud.run_all_queries()
    crud.save_results()
