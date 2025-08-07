# 📊 CONSULTAS CRUD COMPLETAS - MongoDB E-commerce Brasil

## Caso de Estudio: Implementación de Replicación Primario-Secundario

Este proyecto implementa las **15 consultas CRUD** requeridas en el caso de estudio de MongoDB, incluyendo operaciones de lectura, escritura, actualización y eliminación en un entorno de replicación Primario-Secundario.

## 🎯 Objetivos del Caso de Estudio

1. ✅ **Descargar dataset de Kaggle** en formato JSON
2. ✅ **Realizar análisis exploratorio de datos (EDA)** y limpieza (ETL)
3. ✅ **Subir datos procesados a MongoDB**
4. ✅ **Implementar replicación Primario-Secundario**
5. ✅ **Realizar consultas CRUD en entorno de replicación**

## 📋 Consultas CRUD Implementadas

### 🔍 Operaciones de Lectura (Consultas 1-4)

#### **CONSULTA 1: Ventas de los últimos 3 meses para un cliente específico**
- **Descripción**: Devuelve todas las ventas realizadas en los últimos tres meses para un cliente específico, ordenadas por fecha_compra de forma descendente.
- **Campos**: producto, precio, fecha_compra, cliente_id, ciudad
- **Optimización**: Índice compuesto en (id_cliente_unico, fecha_compra)

#### **CONSULTA 2: Total gastado por cliente agrupado por producto**
- **Descripción**: Modificación de la consulta anterior para que también devuelva el total gastado por ese cliente en los tres últimos meses, agrupando las ventas por producto.
- **Agregación**: $group por categoria_producto
- **Métricas**: total_gastado, cantidad_compras, promedio_precio

#### **CONSULTA 3: Productos con stock disminuido más de 15%**
- **Descripción**: Devuelve todos los productos cuya cantidad_stock ha disminuido más de un 15% en comparación con el mes anterior.
- **Lógica**: Comparación mes actual vs mes anterior
- **Cálculo**: Disminución porcentual = ((ventas_mes_anterior - ventas_mes_actual) / ventas_mes_anterior) * 100

#### **CONSULTA 4: Lectura desde nodo secundario**
- **Descripción**: Consulta de lectura desde un nodo secundario para obtener productos vendidos en una ciudad específica con precio por encima del promedio.
- **Replicación**: Demuestra lectura desde secundario
- **Consideraciones**: Consistencia eventual, datos ligeramente desactualizados

### ✏️ Operaciones de Actualización (Consultas 5-7)

#### **CONSULTA 5: Actualizar precio +10% en rango de fechas**
- **Descripción**: Actualiza el precio de todos los productos vendidos en un rango de fechas específico, aumentando el precio un 10%.
- **Operador**: $mul para multiplicar precios
- **Condición**: Rango de fechas específico

#### **CONSULTA 6: Actualizar email de cliente condicionado**
- **Descripción**: Actualizar la dirección de correo electrónico de un cliente utilizando su cliente_id, pero solo si este cliente ha realizado más de 5 compras y la última compra fue realizada en el último trimestre.
- **Condiciones**: $and con $gt para validaciones
- **Verificación**: Pipeline de agregación previo

#### **CONSULTA 7: Actualizar precios productos vendidos >100 veces**
- **Descripción**: Actualizar los precios de todos los productos que hayan sido vendidos más de 100 veces en el último año, pero solo si el precio de esos productos está por debajo de un umbral.
- **Agregación**: Contar ventas por producto
- **Filtro**: Umbral de precio y cantidad de ventas

### 🗑️ Operaciones de Eliminación (Consultas 8-10)

#### **CONSULTA 8: Eliminar productos stock 0 sin ventas 6 meses**
- **Descripción**: Eliminar todos los productos de la colección productos cuya cantidad_stock sea 0 y que no se hayan vendido en los últimos 6 meses.
- **Optimización**: Índice en (cantidad_stock, ultima_venta)
- **Seguridad**: Verificación de ventas recientes

#### **CONSULTA 9: Eliminar ventas ciudad precio bajo promedio**
- **Descripción**: Eliminar todas las ventas de la colección ventas realizadas en una ciudad específica y cuyo precio esté por debajo del promedio de todas las ventas realizadas en esa ciudad en el último trimestre.
- **Cálculo**: Promedio por ciudad y trimestre
- **Replicación**: Operación se propaga a todos los nodos

#### **CONSULTA 10: Eliminar clientes bajo umbral compras**
- **Descripción**: Eliminar todos los clientes cuyo total de compras no ha superado un valor mínimo durante el último año.
- **Agregación**: Suma de compras por cliente
- **Impacto**: Consideraciones en clúster con replicación

### 📊 Agregaciones Avanzadas (Consultas 11-15)

#### **CONSULTA 11: Agregación total ventas por cliente último año**
- **Descripción**: Consulta de agregación para calcular el total de ventas por cliente en el último año, devolviendo cliente_id, total de ventas y promedio de precio por venta.
- **Pipeline**: $match, $group, $sort, $project
- **Optimización**: Índice en fecha_compra

#### **CONSULTA 12: Productos más vendidos último trimestre**
- **Descripción**: Consulta para obtener los productos más vendidos en el último trimestre, devolviendo nombre del producto, cantidad total vendida y total de ingresos generados.
- **Métricas**: cantidad_vendida, ingresos_totales, promedio_precio
- **Ordenamiento**: Por cantidad de ventas descendente

#### **CONSULTA 13: Total ventas por ciudad último mes**
- **Descripción**: Consulta para obtener el total de ventas realizadas por cada ciudad en el último mes, ordenando las ciudades en función de la cantidad de ventas de manera descendente.
- **Agrupación**: Por ciudad_cliente
- **Período**: Último mes (30 días)

#### **CONSULTA 14: Correlación precio vs stock productos**
- **Descripción**: Calcular la correlación entre el precio de los productos y su cantidad_stock utilizando operaciones de agregación.
- **Análisis**: Correlación estadística
- **Filtro**: Solo productos con suficientes datos (>10 ventas)

#### **CONSULTA 15: Top 5 productos más vendidos excluyendo stock bajo**
- **Descripción**: Consulta que devuelve los 5 productos con mayor cantidad de ventas en el último trimestre, excluyendo los productos con menos de 10 unidades en stock.
- **Filtros**: stock_estimado >= 10
- **Límite**: Top 5 productos

## 🚀 Cómo Ejecutar las Consultas

### Opción 1: Script Completo
```bash
# Ejecutar todas las consultas CRUD
python scripts/crud_consultas_completas.py
```

### Opción 2: Demo de Replicación
```bash
# Ejecutar demo específico de replicación
python scripts/demo_replicacion_consultas.py
```

### Opción 3: Notebook Jupyter
```bash
# Generar notebook con todas las consultas
python scripts/generar_notebook_consultas_completas.py

# Abrir notebook
jupyter notebook notebooks/Consultas_CRUD_Completas.ipynb
```

## 🔧 Configuración Requerida

### Prerrequisitos
- Python 3.8+
- MongoDB 4.4+
- Docker (para replicación)
- PyMongo
- Pandas
- NumPy

### Instalación
```bash
# Instalar dependencias
pip install -r requirements.txt

# Iniciar MongoDB con replicación
docker-compose up -d

# Verificar conexión
python scripts/test_connection.py
```

## 📈 Optimizaciones Implementadas

### 1. Índices Compuestos
```javascript
// Índices optimizados para consultas eficientes
[
  [('id_cliente_unico', 1), ('fecha_compra', -1)],
  [('ciudad_cliente', 1), ('fecha_compra', -1)],
  [('precio_total', 1), ('fecha_compra', -1)],
  [('categoria_producto', 1), ('fecha_compra', -1)],
  [('fecha_compra', -1)]
]
```

### 2. Pipeline de Agregación Optimizado
- **$match temprano**: Filtrar datos al inicio del pipeline
- **Proyecciones**: Solo campos necesarios
- **Límites**: Evitar sobrecarga de memoria
- **Ordenamiento eficiente**: Aprovechar índices

### 3. Replicación Primario-Secundario
- **Escrituras**: Solo en nodo primario
- **Lecturas**: Balanceadas entre primario y secundario
- **Consistencia eventual**: Datos sincronizados automáticamente
- **Alta disponibilidad**: Continuidad del servicio

## 📊 Beneficios de la Replicación

### Alta Disponibilidad
- **Continuidad del servicio** durante fallos
- **Recuperación automática** de nodos
- **Redundancia de datos** en múltiples servidores

### Escalabilidad
- **Distribución de carga** de lectura
- **Mejor rendimiento** en consultas concurrentes
- **Balanceo de carga** automático

### Tolerancia a Fallos
- **Failover automático** al secundario
- **Datos protegidos** contra pérdida
- **Backup automático** en tiempo real

## 🔍 Análisis de Rendimiento

### Métricas de Consultas
- **Tiempo de ejecución**: Medido en milisegundos
- **Número de resultados**: Cantidad de documentos procesados
- **Uso de memoria**: Optimizado con proyecciones
- **Escalabilidad**: Pruebas con diferentes volúmenes de datos

### Comparación Primario vs Secundario
- **Rendimiento**: Análisis estadístico de tiempos
- **Consistencia**: Verificación de datos sincronizados
- **Latencia**: Medición de retrasos de replicación

## 📚 Estructura del Proyecto

```
MongoDB_Replicacion_Proyecto/
├── scripts/
│   ├── crud_consultas_completas.py          # Todas las 15 consultas CRUD
│   ├── demo_replicacion_consultas.py        # Demo específico de replicación
│   └── generar_notebook_consultas_completas.py  # Generador de notebook
├── notebooks/
│   ├── Consultas_CRUD_Completas.ipynb       # Notebook con todas las consultas
│   ├── Consultas_CRUD.ipynb                 # Notebook original optimizado
│   ├── EDA_ETL_MongoDB.ipynb                # Análisis exploratorio
│   └── Pruebas_Resiliencia_Replicacion.ipynb # Pruebas de replicación
├── docker/
│   └── docker-compose.yml                   # Configuración de replicación
├── data/                                    # Datasets procesados
└── requirements.txt                         # Dependencias Python
```

## 🎯 Casos de Uso Implementados

### 1. Análisis de Clientes
- Seguimiento de compras por cliente
- Identificación de clientes VIP
- Análisis de patrones de compra

### 2. Gestión de Productos
- Control de stock y ventas
- Identificación de productos más vendidos
- Análisis de rendimiento por categoría

### 3. Análisis Geográfico
- Ventas por ciudad
- Distribución regional de productos
- Análisis de mercado local

### 4. Optimización de Precios
- Ajustes dinámicos de precios
- Análisis de correlación precio-ventas
- Estrategias de pricing

## 🔒 Consideraciones de Seguridad

### Replicación
- **Datos sensibles**: Protegidos en múltiples nodos
- **Acceso controlado**: Autenticación en todos los nodos
- **Backup automático**: Redundancia de datos

### Operaciones CRUD
- **Validación**: Verificación de datos antes de operaciones
- **Transacciones**: Operaciones atómicas cuando es necesario
- **Auditoría**: Logs de todas las operaciones

## 📈 Resultados Esperados

### Rendimiento
- **Consultas optimizadas**: 10-100x más rápido que consultas no optimizadas
- **Menor uso de memoria**: Proyecciones eficientes
- **Mejor escalabilidad**: Índices compuestos

### Confiabilidad
- **Alta disponibilidad**: 99.9% uptime con replicación
- **Consistencia de datos**: Sincronización automática
- **Recuperación rápida**: Failover automático

## 🎉 Conclusión

Este proyecto demuestra exitosamente la implementación de un sistema completo de MongoDB con replicación Primario-Secundario, incluyendo:

✅ **15 consultas CRUD optimizadas** implementadas y probadas
✅ **Replicación funcional** con alta disponibilidad
✅ **Análisis de rendimiento** y optimizaciones
✅ **Documentación completa** y casos de uso
✅ **Scripts ejecutables** y notebooks interactivos

El sistema está listo para producción y puede manejar grandes volúmenes de datos con alta disponibilidad y rendimiento optimizado.

---

**🎯 Caso de Estudio Completado Exitosamente**

Para más información, consultar la documentación de MongoDB o contactar al equipo de desarrollo. 