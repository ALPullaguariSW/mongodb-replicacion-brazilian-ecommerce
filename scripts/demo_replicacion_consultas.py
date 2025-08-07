#!/usr/bin/env python3
"""
🔄 DEMO CONSULTAS DE REPLICACIÓN PRIMARIO-SECUNDARIO
MongoDB E-commerce Brasil

Este script demuestra específicamente las consultas que aprovechan
la replicación Primario-Secundario en MongoDB, incluyendo:
- Escrituras en el primario
- Lecturas desde el secundario
- Verificación de sincronización
- Análisis de consistencia eventual
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient, ReadPreference
from datetime import datetime, timedelta
import time
import json

class DemoReplicacionMongoDB:
    def __init__(self):
        """Inicializar conexiones a MongoDB primario y secundario"""
        print("🔄 INICIANDO DEMO DE REPLICACIÓN PRIMARIO-SECUNDARIO")
        print("=" * 60)
        
        # Conexiones a MongoDB
        self.primary_client = MongoClient('mongodb://localhost:27020/', directConnection=True)
        self.secondary_client = MongoClient('mongodb://localhost:27021/', directConnection=True)
        
        self.primary_db = self.primary_client['ecommerce_brazil']
        self.secondary_db = self.secondary_client['ecommerce_brazil']
        
        self.primary_collection = self.primary_db['ventas']
        self.secondary_collection = self.secondary_db['ventas']
        
        # Verificar conexiones
        self._verificar_conexiones()
        
    def _verificar_conexiones(self):
        """Verificar que las conexiones estén activas"""
        try:
            # Verificar primario
            primary_count = self.primary_collection.count_documents({})
            print(f"✅ Primario conectado - Documentos: {primary_count:,}")
            
            # Verificar secundario
            secondary_count = self.secondary_collection.count_documents({})
            print(f"✅ Secundario conectado - Documentos: {secondary_count:,}")
            
            # Verificar sincronización
            if primary_count == secondary_count:
                print("🔄 Nodos sincronizados correctamente")
            else:
                print("⚠️ Diferencia en número de documentos entre nodos")
                
        except Exception as e:
            print(f"❌ Error de conexión: {e}")
            raise

    def demo_1_escritura_primario_lectura_secundario(self):
        """
        DEMO 1: Escribir en el primario y leer desde el secundario
        """
        print("\n🔄 DEMO 1: Escritura en Primario → Lectura desde Secundario")
        print("-" * 50)
        
        # 1. Insertar documento de prueba en el primario
        documento_prueba = {
            'id_orden': 'demo_replicacion_001',
            'id_cliente_unico': 'cliente_demo_replicacion',
            'categoria_producto': 'demo_producto',
            'precio_total': 99.99,
            'fecha_compra': datetime.now(),
            'ciudad_cliente': 'ciudad_demo',
            'puntuacion_review': 5.0,
            'es_demo': True
        }
        
        print("📝 Insertando documento de prueba en el primario...")
        resultado_insercion = self.primary_collection.insert_one(documento_prueba)
        print(f"✅ Documento insertado con ID: {resultado_insercion.inserted_id}")
        
        # 2. Esperar un momento para la replicación
        print("⏳ Esperando replicación...")
        time.sleep(2)
        
        # 3. Buscar el documento desde el secundario
        print("🔍 Buscando documento desde el secundario...")
        documento_encontrado = self.secondary_collection.find_one({
            'id_orden': 'demo_replicacion_001'
        })
        
        if documento_encontrado:
            print("✅ Documento encontrado en el secundario")
            print(f"   - ID: {documento_encontrado['_id']}")
            print(f"   - Producto: {documento_encontrado['categoria_producto']}")
            print(f"   - Precio: R$ {documento_encontrado['precio_total']}")
            print(f"   - Fecha: {documento_encontrado['fecha_compra']}")
        else:
            print("❌ Documento no encontrado en el secundario")
            print("⚠️ Posible retraso en la replicación")
        
        return documento_encontrado is not None

    def demo_2_lectura_balanceada_primario_secundario(self):
        """
        DEMO 2: Comparar lecturas desde primario vs secundario
        """
        print("\n🔄 DEMO 2: Comparación de Lecturas Primario vs Secundario")
        print("-" * 50)
        
        # Consulta de ejemplo: top 5 clientes por ventas
        pipeline = [
            {
                '$group': {
                    '_id': '$id_cliente_unico',
                    'total_ventas': {'$sum': 1},
                    'total_gastado': {'$sum': '$precio_total'}
                }
            },
            {
                '$sort': {'total_ventas': -1}
            },
            {
                '$limit': 5
            }
        ]
        
        # Lectura desde primario
        print("📊 Leyendo desde PRIMARIO...")
        start_time = time.time()
        resultado_primario = list(self.primary_collection.aggregate(pipeline))
        tiempo_primario = (time.time() - start_time) * 1000
        
        print(f"⏱️ Tiempo primario: {tiempo_primario:.2f}ms")
        print(f"📈 Resultados primario: {len(resultado_primario)} clientes")
        
        # Lectura desde secundario
        print("\n📊 Leyendo desde SECUNDARIO...")
        start_time = time.time()
        resultado_secundario = list(self.secondary_collection.aggregate(pipeline))
        tiempo_secundario = (time.time() - start_time) * 1000
        
        print(f"⏱️ Tiempo secundario: {tiempo_secundario:.2f}ms")
        print(f"📈 Resultados secundario: {len(resultado_secundario)} clientes")
        
        # Comparar resultados
        print("\n🔍 Comparación de resultados:")
        if len(resultado_primario) == len(resultado_secundario):
            print("✅ Mismo número de resultados")
            
            # Comparar datos
            for i, (prim, sec) in enumerate(zip(resultado_primario, resultado_secundario)):
                if prim['_id'] == sec['_id'] and prim['total_ventas'] == sec['total_ventas']:
                    print(f"   Cliente {i+1}: ✅ Sincronizado")
                else:
                    print(f"   Cliente {i+1}: ❌ Diferencia detectada")
        else:
            print("❌ Diferente número de resultados")
        
        # Análisis de rendimiento
        print(f"\n📊 Análisis de rendimiento:")
        if tiempo_secundario < tiempo_primario:
            print(f"🚀 Secundario más rápido: {tiempo_primario - tiempo_secundario:.2f}ms")
        else:
            print(f"🐌 Primario más rápido: {tiempo_secundario - tiempo_primario:.2f}ms")

    def demo_3_consulta_ciudad_precio_sobre_promedio(self):
        """
        DEMO 3: Consulta específica desde secundario - productos en ciudad con precio sobre promedio
        """
        print("\n🔄 DEMO 3: Consulta desde Secundario - Productos Premium por Ciudad")
        print("-" * 50)
        
        ciudad_ejemplo = 'sao paulo'
        
        # 1. Calcular promedio desde el primario
        pipeline_promedio = [
            {'$group': {'_id': None, 'promedio_precio': {'$avg': '$precio_total'}}}
        ]
        
        promedio_result = list(self.primary_collection.aggregate(pipeline_promedio))
        promedio_precio = promedio_result[0]['promedio_precio'] if promedio_result else 0
        
        print(f"💰 Precio promedio general: R$ {promedio_precio:.2f}")
        
        # 2. Consulta desde el secundario
        pipeline = [
            {
                '$match': {
                    'ciudad_cliente': ciudad_ejemplo,
                    'precio_total': {'$gt': promedio_precio}
                }
            },
            {
                '$group': {
                    '_id': '$categoria_producto',
                    'cantidad_productos': {'$sum': 1},
                    'precio_promedio': {'$avg': '$precio_total'},
                    'precio_maximo': {'$max': '$precio_total'},
                    'precio_minimo': {'$min': '$precio_total'}
                }
            },
            {
                '$sort': {'precio_promedio': -1}
            },
            {
                '$limit': 10
            }
        ]
        
        print(f"🔍 Buscando productos premium en {ciudad_ejemplo}...")
        start_time = time.time()
        resultado = list(self.secondary_collection.aggregate(pipeline))
        execution_time = (time.time() - start_time) * 1000
        
        print(f"📊 Productos premium encontrados: {len(resultado)}")
        print(f"⏱️ Tiempo de ejecución: {execution_time:.2f}ms")
        print("⚠️ NOTA: Lectura desde secundario - datos pueden estar ligeramente desactualizados")
        
        if resultado:
            df = pd.DataFrame(resultado)
            print("\n🏆 Top Productos Premium:")
            print(df[['_id', 'cantidad_productos', 'precio_promedio', 'precio_maximo']].head())
        
        return resultado

    def demo_4_verificacion_consistencia_eventual(self):
        """
        DEMO 4: Verificar consistencia eventual entre nodos
        """
        print("\n🔄 DEMO 4: Verificación de Consistencia Eventual")
        print("-" * 50)
        
        # Insertar múltiples documentos de prueba
        documentos_prueba = []
        for i in range(5):
            doc = {
                'id_orden': f'demo_consistencia_{i:03d}',
                'id_cliente_unico': f'cliente_consistencia_{i}',
                'categoria_producto': f'producto_consistencia_{i}',
                'precio_total': 50.0 + i * 10,
                'fecha_compra': datetime.now(),
                'ciudad_cliente': 'ciudad_consistencia',
                'es_consistencia_test': True
            }
            documentos_prueba.append(doc)
        
        print("📝 Insertando 5 documentos de prueba en el primario...")
        resultado_insercion = self.primary_collection.insert_many(documentos_prueba)
        print(f"✅ {len(resultado_insercion.inserted_ids)} documentos insertados")
        
        # Verificar replicación con diferentes intervalos
        intervalos = [1, 3, 5, 10]
        
        for intervalo in intervalos:
            print(f"\n⏳ Esperando {intervalo} segundos...")
            time.sleep(intervalo)
            
            # Contar documentos en ambos nodos
            count_primario = self.primary_collection.count_documents({'es_consistencia_test': True})
            count_secundario = self.secondary_collection.count_documents({'es_consistencia_test': True})
            
            print(f"📊 Después de {intervalo}s:")
            print(f"   Primario: {count_primario} documentos")
            print(f"   Secundario: {count_secundario} documentos")
            
            if count_primario == count_secundario:
                print(f"   ✅ Consistencia alcanzada en {intervalo}s")
                break
            else:
                print(f"   ⏳ Replicación en progreso...")
        
        # Limpiar documentos de prueba
        print("\n🧹 Limpiando documentos de prueba...")
        self.primary_collection.delete_many({'es_consistencia_test': True})
        self.secondary_collection.delete_many({'es_consistencia_test': True})
        print("✅ Documentos de prueba eliminados")

    def demo_5_analisis_rendimiento_replicacion(self):
        """
        DEMO 5: Análisis de rendimiento con replicación
        """
        print("\n🔄 DEMO 5: Análisis de Rendimiento con Replicación")
        print("-" * 50)
        
        # Consulta compleja para medir rendimiento
        pipeline_complejo = [
            {
                '$match': {
                    'fecha_compra': {
                        '$gte': datetime.now() - timedelta(days=365)
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'ciudad': '$ciudad_cliente',
                        'categoria': '$categoria_producto'
                    },
                    'total_ventas': {'$sum': 1},
                    'ingresos_totales': {'$sum': '$precio_total'},
                    'promedio_precio': {'$avg': '$precio_total'},
                    'puntuacion_promedio': {'$avg': '$puntuacion_review'}
                }
            },
            {
                '$sort': {'ingresos_totales': -1}
            },
            {
                '$limit': 20
            }
        ]
        
        # Medir rendimiento en primario
        print("📊 Mediendo rendimiento en PRIMARIO...")
        tiempos_primario = []
        for i in range(5):
            start_time = time.time()
            resultado = list(self.primary_collection.aggregate(pipeline_complejo))
            tiempo = (time.time() - start_time) * 1000
            tiempos_primario.append(tiempo)
            print(f"   Ejecución {i+1}: {tiempo:.2f}ms")
        
        # Medir rendimiento en secundario
        print("\n📊 Mediendo rendimiento en SECUNDARIO...")
        tiempos_secundario = []
        for i in range(5):
            start_time = time.time()
            resultado = list(self.secondary_collection.aggregate(pipeline_complejo))
            tiempo = (time.time() - start_time) * 1000
            tiempos_secundario.append(tiempo)
            print(f"   Ejecución {i+1}: {tiempo:.2f}ms")
        
        # Análisis estadístico
        print("\n📈 Análisis Estadístico:")
        print(f"   Primario - Promedio: {np.mean(tiempos_primario):.2f}ms")
        print(f"   Primario - Desv. Est.: {np.std(tiempos_primario):.2f}ms")
        print(f"   Secundario - Promedio: {np.mean(tiempos_secundario):.2f}ms")
        print(f"   Secundario - Desv. Est.: {np.std(tiempos_secundario):.2f}ms")
        
        if np.mean(tiempos_secundario) < np.mean(tiempos_primario):
            print("🚀 Secundario es más rápido en promedio")
        else:
            print("🐌 Primario es más rápido en promedio")

    def ejecutar_todas_las_demos(self):
        """Ejecutar todas las demostraciones de replicación"""
        print("🚀 EJECUTANDO TODAS LAS DEMOS DE REPLICACIÓN")
        print("=" * 60)
        
        resultados = {}
        
        # Ejecutar demos
        resultados['demo_1'] = self.demo_1_escritura_primario_lectura_secundario()
        resultados['demo_2'] = self.demo_2_lectura_balanceada_primario_secundario()
        resultados['demo_3'] = self.demo_3_consulta_ciudad_precio_sobre_promedio()
        self.demo_4_verificacion_consistencia_eventual()
        self.demo_5_analisis_rendimiento_replicacion()
        
        print("\n" + "=" * 60)
        print("✅ TODAS LAS DEMOS DE REPLICACIÓN COMPLETADAS")
        print("=" * 60)
        
        # Resumen
        print("\n📋 RESUMEN DE RESULTADOS:")
        for demo, resultado in resultados.items():
            if isinstance(resultado, bool):
                status = "✅ EXITOSA" if resultado else "❌ FALLIDA"
                print(f"  {demo}: {status}")
            else:
                print(f"  {demo}: {len(resultado)} resultados")
        
        return resultados

def main():
    """Función principal"""
    print("🔄 DEMO CONSULTAS DE REPLICACIÓN PRIMARIO-SECUNDARIO")
    print("MongoDB E-commerce Brasil")
    print("=" * 60)
    
    try:
        # Inicializar demo
        demo = DemoReplicacionMongoDB()
        
        # Ejecutar todas las demos
        resultados = demo.ejecutar_todas_las_demos()
        
        print("\n🎉 ¡Demo de replicación completada exitosamente!")
        print("\n📚 Para más información sobre replicación:")
        print("  - Documentación MongoDB: https://docs.mongodb.com/manual/replication/")
        print("  - Mejores prácticas: https://docs.mongodb.com/manual/core/replica-set-architecture/")
        
    except Exception as e:
        print(f"❌ Error durante la demo: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 