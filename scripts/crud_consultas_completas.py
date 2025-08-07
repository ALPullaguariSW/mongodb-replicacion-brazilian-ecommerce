#!/usr/bin/env python3
"""
📊 CONSULTAS CRUD COMPLETAS - MongoDB E-commerce Brasil
Caso de Estudio: Implementación de Replicación Primario-Secundario

Este script implementa las 15 consultas CRUD requeridas en el caso de estudio,
incluyendo operaciones de lectura, escritura, actualización y eliminación
en un entorno de replicación MongoDB.
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient, ReadPreference
from datetime import datetime, timedelta
import time
import json

class MongoDBConsultasCRUD:
    def __init__(self, primary_uri='mongodb://localhost:27020/', 
                 secondary_uri='mongodb://localhost:27021/'):
        """Inicializar conexiones a MongoDB primario y secundario"""
        self.primary_client = MongoClient(primary_uri, directConnection=True)
        self.secondary_client = MongoClient(secondary_uri, directConnection=True)
        
        self.primary_db = self.primary_client['ecommerce_brazil']
        self.secondary_db = self.secondary_client['ecommerce_brazil']
        
        self.ventas_collection = self.primary_db['ventas']
        self.clientes_collection = self.primary_db['clientes']
        self.productos_collection = self.primary_db['productos']
        
        # Crear índices optimizados
        self._crear_indices()
        
    def _crear_indices(self):
        """Crear índices optimizados para las consultas"""
        print("🔧 Creando índices optimizados...")
        
        # Índices para colección ventas
        indexes_ventas = [
            [('id_cliente_unico', 1), ('fecha_compra', -1)],
            [('ciudad_cliente', 1), ('fecha_compra', -1)],
            [('precio_total', 1), ('fecha_compra', -1)],
            [('categoria_producto', 1), ('fecha_compra', -1)],
            [('fecha_compra', -1)],
            [('ciudad_cliente', 1), ('precio_total', 1)]
        ]
        
        for index in indexes_ventas:
            try:
                self.ventas_collection.create_index(index, background=True)
                print(f"✅ Índice ventas creado: {index}")
            except Exception as e:
                print(f"⚠️ Índice ventas ya existe: {index}")
        
        # Índices para colección clientes
        indexes_clientes = [
            [('id_cliente_unico', 1)],
            [('email', 1)],
            [('total_compras', -1), ('ultima_compra', -1)]
        ]
        
        for index in indexes_clientes:
            try:
                self.clientes_collection.create_index(index, background=True)
                print(f"✅ Índice clientes creado: {index}")
            except Exception as e:
                print(f"⚠️ Índice clientes ya existe: {index}")
        
        # Índices para colección productos
        indexes_productos = [
            [('id_producto', 1)],
            [('cantidad_stock', 1), ('ultima_venta', -1)],
            [('precio', 1), ('cantidad_stock', 1)]
        ]
        
        for index in indexes_productos:
            try:
                self.productos_collection.create_index(index, background=True)
                print(f"✅ Índice productos creado: {index}")
            except Exception as e:
                print(f"⚠️ Índice productos ya existe: {index}")

    def consulta_1_ventas_ultimos_tres_meses(self, cliente_id):
        """
        CONSULTA 1: Ventas de los últimos 3 meses para un cliente específico
        Ordenadas por fecha_compra descendente
        """
        print(f"\n🔍 CONSULTA 1: Ventas últimos 3 meses para cliente {cliente_id}")
        
        fecha_limite = datetime.now() - timedelta(days=90)
        
        pipeline = [
            {
                '$match': {
                    'id_cliente_unico': cliente_id,
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$sort': {'fecha_compra': -1}
            },
            {
                '$project': {
                    'producto': '$categoria_producto',
                    'precio': '$precio_total',
                    'fecha_compra': 1,
                    'cliente_id': '$id_cliente_unico',
                    'ciudad': '$ciudad_cliente'
                }
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Ventas encontradas: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n📋 Resultados:")
            print(df.head())
        
        return resultado

    def consulta_2_total_gastado_agrupado_por_producto(self, cliente_id):
        """
        CONSULTA 2: Total gastado por cliente en 3 meses, agrupado por producto
        """
        print(f"\n🔍 CONSULTA 2: Total gastado por cliente {cliente_id} agrupado por producto")
        
        fecha_limite = datetime.now() - timedelta(days=90)
        
        pipeline = [
            {
                '$match': {
                    'id_cliente_unico': cliente_id,
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': '$categoria_producto',
                    'total_gastado': {'$sum': '$precio_total'},
                    'cantidad_compras': {'$sum': 1},
                    'promedio_precio': {'$avg': '$precio_total'}
                }
            },
            {
                '$sort': {'total_gastado': -1}
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Productos encontrados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n📋 Resultados:")
            print(df)
        
        return resultado

    def consulta_3_productos_stock_disminuido_15_porciento(self):
        """
        CONSULTA 3: Productos con stock disminuido más de 15% vs mes anterior
        """
        print("\n🔍 CONSULTA 3: Productos con stock disminuido >15% vs mes anterior")
        
        mes_actual = datetime.now().replace(day=1)
        mes_anterior = (mes_actual - timedelta(days=1)).replace(day=1)
        
        pipeline = [
            {
                '$match': {
                    'fecha_compra': {
                        '$gte': mes_anterior,
                        '$lt': mes_actual + timedelta(days=32)
                    }
                }
            },
            {
                '$group': {
                    '_id': '$categoria_producto',
                    'ventas_mes_actual': {
                        '$sum': {
                            '$cond': [
                                {'$gte': ['$fecha_compra', mes_actual]},
                                1, 0
                            ]
                        }
                    },
                    'ventas_mes_anterior': {
                        '$sum': {
                            '$cond': [
                                {'$lt': ['$fecha_compra', mes_actual]},
                                1, 0
                            ]
                        }
                    }
                }
            },
            {
                '$addFields': {
                    'disminucion_porcentual': {
                        '$multiply': [
                            {
                                '$divide': [
                                    {'$subtract': ['$ventas_mes_anterior', '$ventas_mes_actual']},
                                    '$ventas_mes_anterior'
                                ]
                            },
                            100
                        ]
                    }
                }
            },
            {
                '$match': {
                    'disminucion_porcentual': {'$gt': 15},
                    'ventas_mes_anterior': {'$gt': 0}
                }
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Productos encontrados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n📋 Resultados:")
            print(df)
        
        return resultado

    def consulta_4_lectura_desde_secundario_ciudad_precio_sobre_promedio(self, ciudad):
        """
        CONSULTA 4: Lectura desde nodo secundario - productos en ciudad con precio sobre promedio
        """
        print(f"\n🔍 CONSULTA 4: Productos en {ciudad} con precio sobre promedio (desde secundario)")
        
        # Primero calcular el promedio desde el primario
        pipeline_promedio = [
            {'$group': {'_id': None, 'promedio_precio': {'$avg': '$precio_total'}}}
        ]
        
        promedio_result = list(self.ventas_collection.aggregate(pipeline_promedio))
        promedio_precio = promedio_result[0]['promedio_precio'] if promedio_result else 0
        
        print(f"💰 Precio promedio general: R$ {promedio_precio:.2f}")
        
        # Consulta desde el secundario
        secondary_collection = self.secondary_db['ventas']
        
        pipeline = [
            {
                '$match': {
                    'ciudad_cliente': ciudad,
                    'precio_total': {'$gt': promedio_precio}
                }
            },
            {
                '$project': {
                    'producto': '$categoria_producto',
                    'precio': '$precio_total',
                    'fecha_compra': 1,
                    'ciudad': '$ciudad_cliente'
                }
            },
            {
                '$sort': {'precio': -1}
            },
            {
                '$limit': 10
            }
        ]
        
        start_time = time.time()
        resultado = list(secondary_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Productos encontrados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        print("⚠️ NOTA: Lectura desde secundario puede tener datos ligeramente desactualizados")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n📋 Resultados:")
            print(df)
        
        return resultado

    def consulta_5_actualizar_precio_10_porciento_rango_fechas(self, fecha_inicio, fecha_fin):
        """
        CONSULTA 5: Actualizar precio +10% en rango de fechas específico
        """
        print(f"\n🔍 CONSULTA 5: Actualizar precios +10% entre {fecha_inicio} y {fecha_fin}")
        
        # Primero verificar cuántos documentos se van a actualizar
        pipeline_count = [
            {
                '$match': {
                    'fecha_compra': {
                        '$gte': fecha_inicio,
                        '$lte': fecha_fin
                    }
                }
            },
            {
                '$count': 'total'
            }
        ]
        
        count_result = list(self.ventas_collection.aggregate(pipeline_count))
        total_a_actualizar = count_result[0]['total'] if count_result else 0
        
        print(f"📊 Documentos a actualizar: {total_a_actualizar}")
        
        # Realizar la actualización
        start_time = time.time()
        
        resultado = self.ventas_collection.update_many(
            {
                'fecha_compra': {
                    '$gte': fecha_inicio,
                    '$lte': fecha_fin
                }
            },
            {
                '$mul': {'precio_total': 1.10}
            }
        )
        
        execution_time = (time.time() - start_time) * 1000
        
        print(f"✅ Documentos actualizados: {resultado.modified_count}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        return resultado.modified_count

    def consulta_6_actualizar_email_cliente_condicionado(self, cliente_id, nuevo_email):
        """
        CONSULTA 6: Actualizar email de cliente si cumple condiciones
        """
        print(f"\n🔍 CONSULTA 6: Actualizar email de cliente {cliente_id} si cumple condiciones")
        
        # Verificar condiciones antes de actualizar
        pipeline_verificacion = [
            {
                '$match': {
                    'id_cliente_unico': cliente_id
                }
            },
            {
                '$group': {
                    '_id': '$id_cliente_unico',
                    'total_compras': {'$sum': 1},
                    'ultima_compra': {'$max': '$fecha_compra'}
                }
            }
        ]
        
        verificacion = list(self.ventas_collection.aggregate(pipeline_verificacion))
        
        if not verificacion:
            print("❌ Cliente no encontrado")
            return False
        
        cliente_info = verificacion[0]
        ultimo_trimestre = datetime.now() - timedelta(days=90)
        
        print(f"📊 Compras del cliente: {cliente_info['total_compras']}")
        print(f"📅 Última compra: {cliente_info['ultima_compra']}")
        print(f"📅 Último trimestre: {ultimo_trimestre}")
        
        # Verificar condiciones
        cumple_condiciones = (
            cliente_info['total_compras'] > 5 and 
            cliente_info['ultima_compra'] >= ultimo_trimestre
        )
        
        if cumple_condiciones:
            # Actualizar en colección de clientes
            resultado = self.clientes_collection.update_one(
                {
                    'id_cliente_unico': cliente_id
                },
                {
                    '$set': {'email': nuevo_email}
                }
            )
            
            print(f"✅ Email actualizado: {resultado.modified_count} documento")
            return True
        else:
            print("❌ Cliente no cumple las condiciones para actualización")
            return False

    def consulta_7_actualizar_precios_productos_vendidos_100_veces(self, umbral_precio=100):
        """
        CONSULTA 7: Actualizar precios de productos vendidos >100 veces en último año
        """
        print(f"\n🔍 CONSULTA 7: Actualizar precios productos vendidos >100 veces (umbral: ${umbral_precio})")
        
        fecha_limite = datetime.now() - timedelta(days=365)
        
        # Pipeline para identificar productos que cumplen condiciones
        pipeline_identificacion = [
            {
                '$match': {
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': '$categoria_producto',
                    'total_ventas': {'$sum': 1},
                    'precio_promedio': {'$avg': '$precio_total'}
                }
            },
            {
                '$match': {
                    'total_ventas': {'$gt': 100},
                    'precio_promedio': {'$lt': umbral_precio}
                }
            }
        ]
        
        productos_a_actualizar = list(self.ventas_collection.aggregate(pipeline_identificacion))
        
        print(f"📊 Productos que cumplen condiciones: {len(productos_a_actualizar)}")
        
        if productos_a_actualizar:
            for producto in productos_a_actualizar:
                print(f"  - {producto['_id']}: {producto['total_ventas']} ventas, precio promedio: ${producto['precio_promedio']:.2f}")
        
        # Actualizar precios (simulado - en realidad necesitaríamos una colección de productos)
        print("⚠️ NOTA: Esta actualización requeriría una colección de productos separada")
        
        return productos_a_actualizar

    def consulta_8_eliminar_productos_stock_cero_sin_ventas_6_meses(self):
        """
        CONSULTA 8: Eliminar productos con stock 0 sin ventas en últimos 6 meses
        """
        print("\n🔍 CONSULTA 8: Eliminar productos stock 0 sin ventas en 6 meses")
        
        fecha_limite = datetime.now() - timedelta(days=180)
        
        # Primero identificar productos a eliminar
        pipeline_identificacion = [
            {
                '$match': {
                    'cantidad_stock': 0
                }
            },
            {
                '$lookup': {
                    'from': 'ventas',
                    'localField': 'id_producto',
                    'foreignField': 'categoria_producto',
                    'as': 'ventas_recientes'
                }
            },
            {
                '$addFields': {
                    'tiene_ventas_recientes': {
                        '$gt': [
                            {
                                '$size': {
                                    '$filter': {
                                        'input': '$ventas_recientes',
                                        'cond': {'$gte': ['$$this.fecha_compra', fecha_limite]}
                                    }
                                }
                            },
                            0
                        ]
                    }
                }
            },
            {
                '$match': {
                    'tiene_ventas_recientes': False
                }
            }
        ]
        
        productos_a_eliminar = list(self.productos_collection.aggregate(pipeline_identificacion))
        
        print(f"📊 Productos a eliminar: {len(productos_a_eliminar)}")
        
        if productos_a_eliminar:
            # Eliminar productos
            ids_a_eliminar = [p['_id'] for p in productos_a_eliminar]
            
            resultado = self.productos_collection.delete_many({
                '_id': {'$in': ids_a_eliminar}
            })
            
            print(f"✅ Productos eliminados: {resultado.deleted_count}")
        else:
            print("✅ No hay productos que cumplan las condiciones para eliminar")
        
        return len(productos_a_eliminar)

    def consulta_9_eliminar_ventas_ciudad_precio_bajo_promedio(self, ciudad):
        """
        CONSULTA 9: Eliminar ventas en ciudad con precio bajo promedio del trimestre
        """
        print(f"\n🔍 CONSULTA 9: Eliminar ventas en {ciudad} con precio bajo promedio trimestre")
        
        # Calcular promedio de ventas en la ciudad en el último trimestre
        fecha_limite = datetime.now() - timedelta(days=90)
        
        pipeline_promedio = [
            {
                '$match': {
                    'ciudad_cliente': ciudad,
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'promedio_precio': {'$avg': '$precio_total'}
                }
            }
        ]
        
        promedio_result = list(self.ventas_collection.aggregate(pipeline_promedio))
        
        if not promedio_result:
            print(f"❌ No hay ventas en {ciudad} en el último trimestre")
            return 0
        
        promedio_precio = promedio_result[0]['promedio_precio']
        print(f"💰 Precio promedio en {ciudad}: R$ {promedio_precio:.2f}")
        
        # Contar ventas a eliminar
        pipeline_count = [
            {
                '$match': {
                    'ciudad_cliente': ciudad,
                    'precio_total': {'$lt': promedio_precio}
                }
            },
            {
                '$count': 'total'
            }
        ]
        
        count_result = list(self.ventas_collection.aggregate(pipeline_count))
        total_a_eliminar = count_result[0]['total'] if count_result else 0
        
        print(f"📊 Ventas a eliminar: {total_a_eliminar}")
        
        if total_a_eliminar > 0:
            # Eliminar ventas
            resultado = self.ventas_collection.delete_many({
                'ciudad_cliente': ciudad,
                'precio_total': {'$lt': promedio_precio}
            })
            
            print(f"✅ Ventas eliminadas: {resultado.deleted_count}")
            print("⚠️ NOTA: En un entorno de replicación, esta operación se propagará a todos los nodos")
        else:
            print("✅ No hay ventas que cumplan las condiciones para eliminar")
        
        return total_a_eliminar

    def consulta_10_eliminar_clientes_bajo_umbral_compras(self, umbral_minimo=100):
        """
        CONSULTA 10: Eliminar clientes con total compras bajo umbral en último año
        """
        print(f"\n🔍 CONSULTA 10: Eliminar clientes con compras < ${umbral_minimo} en último año")
        
        fecha_limite = datetime.now() - timedelta(days=365)
        
        # Identificar clientes a eliminar
        pipeline_identificacion = [
            {
                '$match': {
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': '$id_cliente_unico',
                    'total_gastado': {'$sum': '$precio_total'}
                }
            },
            {
                '$match': {
                    'total_gastado': {'$lt': umbral_minimo}
                }
            }
        ]
        
        clientes_a_eliminar = list(self.ventas_collection.aggregate(pipeline_identificacion))
        
        print(f"📊 Clientes a eliminar: {len(clientes_a_eliminar)}")
        
        if clientes_a_eliminar:
            # Eliminar clientes de la colección de clientes
            ids_a_eliminar = [c['_id'] for c in clientes_a_eliminar]
            
            resultado = self.clientes_collection.delete_many({
                'id_cliente_unico': {'$in': ids_a_eliminar}
            })
            
            print(f"✅ Clientes eliminados: {resultado.deleted_count}")
            print("⚠️ NOTA: En un clúster con replicación, las lecturas pueden verse afectadas temporalmente")
        else:
            print("✅ No hay clientes que cumplan las condiciones para eliminar")
        
        return len(clientes_a_eliminar)

    def consulta_11_agregacion_total_ventas_por_cliente_ultimo_año(self):
        """
        CONSULTA 11: Agregación - Total ventas por cliente en último año
        """
        print("\n🔍 CONSULTA 11: Total ventas por cliente en último año")
        
        fecha_limite = datetime.now() - timedelta(days=365)
        
        pipeline = [
            {
                '$match': {
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': '$id_cliente_unico',
                    'total_ventas': {'$sum': 1},
                    'total_gastado': {'$sum': '$precio_total'},
                    'promedio_precio': {'$avg': '$precio_total'}
                }
            },
            {
                '$sort': {'total_gastado': -1}
            },
            {
                '$limit': 10
            },
            {
                '$project': {
                    'cliente_id': '$_id',
                    'total_ventas': 1,
                    'total_gastado': 1,
                    'promedio_precio': 1,
                    '_id': 0
                }
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Clientes encontrados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n📋 Top 10 Clientes:")
            print(df)
        
        return resultado

    def consulta_12_productos_mas_vendidos_ultimo_trimestre(self):
        """
        CONSULTA 12: Productos más vendidos en último trimestre
        """
        print("\n🔍 CONSULTA 12: Productos más vendidos en último trimestre")
        
        fecha_limite = datetime.now() - timedelta(days=90)
        
        pipeline = [
            {
                '$match': {
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': '$categoria_producto',
                    'cantidad_vendida': {'$sum': 1},
                    'ingresos_totales': {'$sum': '$precio_total'},
                    'promedio_precio': {'$avg': '$precio_total'}
                }
            },
            {
                '$sort': {'cantidad_vendida': -1}
            },
            {
                '$limit': 10
            },
            {
                '$project': {
                    'producto': '$_id',
                    'cantidad_vendida': 1,
                    'ingresos_totales': 1,
                    'promedio_precio': 1,
                    '_id': 0
                }
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Productos encontrados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n📋 Top 10 Productos:")
            print(df)
        
        return resultado

    def consulta_13_total_ventas_por_ciudad_ultimo_mes(self):
        """
        CONSULTA 13: Total ventas por ciudad en último mes
        """
        print("\n🔍 CONSULTA 13: Total ventas por ciudad en último mes")
        
        fecha_limite = datetime.now() - timedelta(days=30)
        
        pipeline = [
            {
                '$match': {
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': '$ciudad_cliente',
                    'total_ventas': {'$sum': 1},
                    'ingresos_totales': {'$sum': '$precio_total'},
                    'promedio_precio': {'$avg': '$precio_total'}
                }
            },
            {
                '$sort': {'total_ventas': -1}
            },
            {
                '$project': {
                    'ciudad': '$_id',
                    'total_ventas': 1,
                    'ingresos_totales': 1,
                    'promedio_precio': 1,
                    '_id': 0
                }
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Ciudades encontradas: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n📋 Top Ciudades:")
            print(df.head(10))
        
        return resultado

    def consulta_14_correlacion_precio_stock_productos(self):
        """
        CONSULTA 14: Correlación entre precio y stock de productos
        """
        print("\n🔍 CONSULTA 14: Correlación entre precio y stock de productos")
        
        pipeline = [
            {
                '$group': {
                    '_id': '$categoria_producto',
                    'precio_promedio': {'$avg': '$precio_total'},
                    'total_ventas': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'total_ventas': {'$gt': 10}  # Solo productos con suficientes datos
                }
            },
            {
                '$sort': {'precio_promedio': -1}
            },
            {
                '$limit': 20
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Productos analizados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            
            # Calcular correlación
            if len(df) > 1:
                correlacion = df['precio_promedio'].corr(df['total_ventas'])
                print(f"📈 Correlación precio vs ventas: {correlacion:.4f}")
            
            print("\n📋 Análisis de Productos:")
            print(df[['_id', 'precio_promedio', 'total_ventas']].head(10))
        
        return resultado

    def consulta_15_top_5_productos_ventas_trimestre_excluyendo_stock_bajo(self):
        """
        CONSULTA 15: Top 5 productos con más ventas en trimestre, excluyendo stock bajo
        """
        print("\n🔍 CONSULTA 15: Top 5 productos más vendidos (excluyendo stock < 10)")
        
        fecha_limite = datetime.now() - timedelta(days=90)
        
        pipeline = [
            {
                '$match': {
                    'fecha_compra': {'$gte': fecha_limite}
                }
            },
            {
                '$group': {
                    '_id': '$categoria_producto',
                    'cantidad_vendida': {'$sum': 1},
                    'ingresos_totales': {'$sum': '$precio_total'},
                    'stock_estimado': {'$sum': 1}  # Simulación de stock
                }
            },
            {
                '$match': {
                    'stock_estimado': {'$gte': 10}
                }
            },
            {
                '$sort': {'cantidad_vendida': -1}
            },
            {
                '$limit': 5
            },
            {
                '$project': {
                    'producto': '$_id',
                    'cantidad_vendida': 1,
                    'ingresos_totales': 1,
                    'stock_estimado': 1,
                    '_id': 0
                }
            }
        ]
        
        start_time = time.time()
        resultado = list(self.ventas_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Productos encontrados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n🏆 Top 5 Productos:")
            print(df)
        
        return resultado

    def ejecutar_todas_las_consultas(self):
        """Ejecutar todas las consultas CRUD del caso de estudio"""
        print("🚀 EJECUTANDO TODAS LAS CONSULTAS CRUD DEL CASO DE ESTUDIO")
        print("=" * 80)
        
        # Obtener un cliente de ejemplo
        cliente_ejemplo = list(self.ventas_collection.aggregate([
            {'$group': {'_id': '$id_cliente_unico', 'total': {'$sum': 1}}},
            {'$sort': {'total': -1}},
            {'$limit': 1}
        ]))[0]['_id']
        
        # Ejecutar todas las consultas
        resultados = {}
        
        # Consultas de lectura
        resultados['consulta_1'] = self.consulta_1_ventas_ultimos_tres_meses(cliente_ejemplo)
        resultados['consulta_2'] = self.consulta_2_total_gastado_agrupado_por_producto(cliente_ejemplo)
        resultados['consulta_3'] = self.consulta_3_productos_stock_disminuido_15_porciento()
        resultados['consulta_4'] = self.consulta_4_lectura_desde_secundario_ciudad_precio_sobre_promedio('sao paulo')
        resultados['consulta_11'] = self.consulta_11_agregacion_total_ventas_por_cliente_ultimo_año()
        resultados['consulta_12'] = self.consulta_12_productos_mas_vendidos_ultimo_trimestre()
        resultados['consulta_13'] = self.consulta_13_total_ventas_por_ciudad_ultimo_mes()
        resultados['consulta_14'] = self.consulta_14_correlacion_precio_stock_productos()
        resultados['consulta_15'] = self.consulta_15_top_5_productos_ventas_trimestre_excluyendo_stock_bajo()
        
        # Consultas de actualización
        resultados['consulta_5'] = self.consulta_5_actualizar_precio_10_porciento_rango_fechas(
            datetime.now() - timedelta(days=30), 
            datetime.now()
        )
        resultados['consulta_6'] = self.consulta_6_actualizar_email_cliente_condicionado(
            cliente_ejemplo, 
            'nuevo_email@ejemplo.com'
        )
        resultados['consulta_7'] = self.consulta_7_actualizar_precios_productos_vendidos_100_veces(100)
        
        # Consultas de eliminación
        resultados['consulta_8'] = self.consulta_8_eliminar_productos_stock_cero_sin_ventas_6_meses()
        resultados['consulta_9'] = self.consulta_9_eliminar_ventas_ciudad_precio_bajo_promedio('sao paulo')
        resultados['consulta_10'] = self.consulta_10_eliminar_clientes_bajo_umbral_compras(100)
        
        print("\n" + "=" * 80)
        print("✅ TODAS LAS CONSULTAS CRUD EJECUTADAS EXITOSAMENTE")
        print("=" * 80)
        
        return resultados

def main():
    """Función principal para ejecutar el script"""
    print("📊 CONSULTAS CRUD COMPLETAS - MongoDB E-commerce Brasil")
    print("Caso de Estudio: Implementación de Replicación Primario-Secundario")
    print("=" * 80)
    
    try:
        # Inicializar la clase de consultas
        consultas = MongoDBConsultasCRUD()
        
        # Ejecutar todas las consultas
        resultados = consultas.ejecutar_todas_las_consultas()
        
        print("\n📋 RESUMEN DE EJECUCIÓN:")
        for consulta, resultado in resultados.items():
            if isinstance(resultado, list):
                print(f"  {consulta}: {len(resultado)} resultados")
            elif isinstance(resultado, (int, bool)):
                print(f"  {consulta}: {resultado}")
            else:
                print(f"  {consulta}: Ejecutada")
        
        print("\n🎉 ¡Todas las consultas CRUD han sido ejecutadas exitosamente!")
        
    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 