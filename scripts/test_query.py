#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para probar la consulta corregida
"""

from pymongo import MongoClient
from datetime import datetime

def test_query():
    """Probar la consulta corregida"""
    
    print("🧪 Probando consulta corregida...")
    
    # Conectar a MongoDB
    client = MongoClient('mongodb://localhost:27020/', directConnection=True)
    db = client['ecommerce_brazil']
    collection = db['ventas']
    
    # Cliente que existe en el dataset
    cliente_ejemplo = '0000366f3b9a7992bf8c76cfdf3221e2'
    
    # Fecha de hace 3 meses (ajustada para el dataset)
    fecha_limite = datetime(2018, 2, 1)  # 3 meses antes del final del dataset (2018-05)
    
    # Verificar que el cliente existe
    cliente_count = collection.count_documents({'id_cliente_unico': cliente_ejemplo})
    print(f"📊 Cliente {cliente_ejemplo} tiene {cliente_count} ventas totales")
    
    # Consulta corregida
    query = {
        'id_cliente_unico': cliente_ejemplo,
        'fecha_compra': {'$gte': fecha_limite}
    }
    
    result = list(collection.find(query).sort('fecha_compra', -1))
    print(f"📊 Ventas para cliente {cliente_ejemplo} en los últimos 3 meses: {len(result)}")
    
    if result:
        print("✅ Consulta exitosa - Encontradas ventas:")
        for doc in result[:3]:  # Mostrar solo las primeras 3
            print(f"   - Orden: {doc['id_orden']}, Fecha: {doc['fecha_compra']}, Precio: {doc['precio_total']}")
    else:
        print("❌ No se encontraron ventas en el rango de fechas")
    
    # Verificar el rango de fechas en el dataset
    min_date = collection.find().sort('fecha_compra', 1).limit(1)[0]['fecha_compra']
    max_date = collection.find().sort('fecha_compra', -1).limit(1)[0]['fecha_compra']
    print(f"📅 Rango de fechas en el dataset: {min_date} a {max_date}")
    
    client.close()

if __name__ == "__main__":
    test_query() 