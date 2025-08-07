# 🚀 Proyecto de Replicación MongoDB - E-commerce Brasil

## 📋 Descripción del Proyecto

Este proyecto implementa un **sistema de replicación MongoDB** con un dataset de e-commerce brasileño, incluyendo:

- 🔄 **Replica Set MongoDB** (Primary + 2 Secondary)
- 📊 **ETL completo** con dataset de Kaggle (Brazilian E-commerce)
- 🔍 **Consultas CRUD** avanzadas
- 🧪 **Pruebas de resiliencia** y failover
- 📈 **Análisis exploratorio** de datos
- 🐳 **Docker Compose** para despliegue fácil

## 🎯 Objetivos del Proyecto

1. **Implementar replicación MongoDB** con alta disponibilidad
2. **Realizar EDA y ETL** de datos de e-commerce
3. **Ejecutar operaciones CRUD** complejas
4. **Probar resiliencia** del sistema
5. **Demostrar** capacidades de MongoDB para análisis de datos

## 🛠️ Tecnologías Utilizadas

- **MongoDB 6.0** - Base de datos NoSQL
- **Docker & Docker Compose** - Contenedores
- **Python 3.13** - Análisis de datos
- **Jupyter Notebooks** - Análisis interactivo
- **Pandas, NumPy, Matplotlib** - Data Science
- **PyMongo** - Driver de MongoDB

## 📁 Estructura del Proyecto

```
MongoDB_Replicacion_Proyecto/
├── 📁 data/                          # Datasets y archivos de datos
│   ├── kaggle.json                   # Credenciales de Kaggle
│   └── ventas.json                   # Dataset procesado
├── 📁 docker/                        # Configuración Docker
│   ├── docker-compose.yml           # Servicios MongoDB
│   └── initReplica.js               # Script de inicialización
├── 📁 notebooks/                     # Jupyter Notebooks
│   ├── EDA_ETL_MongoDB.ipynb        # EDA y ETL completo
│   ├── Consultas_CRUD.ipynb         # 15 consultas CRUD
│   └── Pruebas_Resiliencia_Replicacion.ipynb  # Pruebas de failover
└── 📁 scripts/                       # Scripts de utilidad
```

## 🚀 Instalación y Configuración

### 📋 Prerrequisitos

1. **Docker Desktop** instalado y funcionando
2. **Python 3.13** o superior
3. **Git** para clonar el repositorio
4. **Cuenta de Kaggle** (opcional, para descargar dataset)

### 🔧 Pasos de Instalación

#### 1. Clonar el Repositorio
```bash
git clone <URL_DEL_REPOSITORIO>
cd MongoDB_Replicacion_Proyecto
```

#### 2. Configurar Entorno Python
```bash
# Crear entorno virtual (recomendado)
python -m venv venv

# Activar entorno virtual
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

#### 3. Configurar Credenciales de Kaggle (Opcional)
Si quieres descargar el dataset original:
```bash
# Crear archivo kaggle.json en data/
# Obtener credenciales desde: https://www.kaggle.com/settings/account
```

#### 4. Levantar Cluster MongoDB
```bash
cd docker
docker-compose up -d
```

#### 5. Verificar Replica Set
```bash
# Verificar estado del replica set
docker exec -it mongo-primary mongosh --eval "rs.status()"

# Verificar conexión a cada nodo
docker exec -it mongo-primary mongosh --eval "db.adminCommand('ping')"
docker exec -it mongo-secondary1 mongosh --eval "db.adminCommand('ping')"
docker exec -it mongo-secondary2 mongosh --eval "db.adminCommand('ping')"
```

## 📊 Uso del Proyecto

### 🔄 Estado del Replica Set

El proyecto incluye 3 nodos MongoDB:
- **Primary**: `localhost:27020`
- **Secondary 1**: `localhost:27021`
- **Secondary 2**: `localhost:27022`

### 📈 Notebooks Disponibles

#### 1. EDA_ETL_MongoDB.ipynb
- **Descarga automática** del dataset de Kaggle
- **ETL completo** con JOINs optimizados
- **Análisis exploratorio** con visualizaciones
- **Carga de datos** en MongoDB con optimización de memoria

#### 2. Consultas_CRUD.ipynb
- **15 consultas CRUD** avanzadas
- **Análisis de ventas** por diferentes criterios
- **Consultas de clientes** y productos
- **Agregaciones** complejas

#### 3. Pruebas_Resiliencia_Replicacion.ipynb
- **Simulación de fallos** del nodo primario
- **Verificación de failover** automático
- **Pruebas de consistencia** de datos
- **Análisis de disponibilidad** del sistema

### 🎯 Ejecutar Notebooks

```bash
# Iniciar Jupyter
jupyter notebook

# O ejecutar directamente
jupyter notebook notebooks/EDA_ETL_MongoDB.ipynb
```

## 🔍 Consultas de Ejemplo

### Conexión a MongoDB
```python
from pymongo import MongoClient

# Conectar al primario
client = MongoClient('mongodb://localhost:27020/', directConnection=True)
db = client['ecommerce_brazil']
collection = db['ventas']
```

### Consultas Básicas
```python
# Contar documentos
total_ventas = collection.count_documents({})

# Ventas por categoría
ventas_por_categoria = collection.aggregate([
    {"$group": {"_id": "$categoria_producto", "total": {"$sum": 1}}},
    {"$sort": {"total": -1}}
])

# Top 10 clientes
top_clientes = collection.aggregate([
    {"$group": {"_id": "$id_cliente_unico", "total_gastado": {"$sum": "$precio_total"}}},
    {"$sort": {"total_gastado": -1}},
    {"$limit": 10}
])
```

## 🧪 Pruebas de Resiliencia

### Simular Fallo del Primary
```bash
# Detener nodo primario
docker stop mongo-primary

# Verificar elección de nuevo primario
docker exec -it mongo-secondary1 mongosh --eval "rs.status()"

# Reiniciar nodo original
docker start mongo-primary
```

### Verificar Replicación
```bash
# Verificar datos en todos los nodos
docker exec -it mongo-primary mongosh ecommerce_brazil --eval "db.ventas.countDocuments()"
docker exec -it mongo-secondary1 mongosh ecommerce_brazil --eval "db.ventas.countDocuments()"
docker exec -it mongo-secondary2 mongosh ecommerce_brazil --eval "db.ventas.countDocuments()"
```

## 📊 Dataset

### Estructura de Datos
El dataset incluye información completa de transacciones de e-commerce:
- **Órdenes**: 118,310 transacciones
- **Clientes**: Información demográfica
- **Productos**: Catálogo con categorías
- **Vendedores**: Información de ubicación
- **Pagos**: Métodos y valores
- **Reviews**: Evaluaciones de clientes

### Campos Principales
```json
{
  "id_orden": "00010242fe8c5a6d1ba2dd792cb16214",
  "id_cliente_unico": "871766c5855e863f6eccc05f988b23cb",
  "categoria_producto": "cool_stuff",
  "precio_total": 72.19,
  "fecha_compra": "2017-09-13T08:59:02.000Z",
  "ciudad_cliente": "campos dos goytacazes",
  "estado_cliente": "RJ",
  "puntuacion_review": 5
}
```

## 🔧 Solución de Problemas

### Error de Puerto Ocupado
```bash
# Verificar puertos en uso
netstat -an | findstr :27020
netstat -an | findstr :27021
netstat -an | findstr :27022

# Si hay conflicto, cambiar puertos en docker-compose.yml
```

### Error de Montaje Docker
```bash
# Si hay problemas con volúmenes
docker-compose down
docker volume prune
docker-compose up -d
```

### Problemas de Memoria
```bash
# Si el ETL falla por memoria
# Reducir CHUNK_SIZE en el notebook
# O ejecutar en chunks más pequeños
```

### Replica Set No Inicializado
```bash
# Reinicializar replica set
docker exec -it mongo-primary mongosh --eval "rs.initiate()"
```

## 📈 Resultados Esperados

### Después de la Ejecución Completa:
- ✅ **118,310 documentos** en la colección `ventas`
- ✅ **3 nodos MongoDB** funcionando en replica set
- ✅ **Datos replicados** en todos los nodos
- ✅ **15 consultas CRUD** ejecutándose correctamente
- ✅ **Pruebas de resiliencia** exitosas

### Métricas de Rendimiento:
- **Tiempo de carga**: ~5-10 minutos (dependiendo del hardware)
- **Tamaño de datos**: ~50MB en MongoDB
- **Consultas**: <1 segundo para consultas simples
- **Failover**: <10 segundos para elección de nuevo primario

## 🤝 Contribuciones

Para contribuir al proyecto:

1. Fork el repositorio
2. Crear una rama para tu feature
3. Commit tus cambios
4. Push a la rama
5. Crear un Pull Request

## 📝 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

## 👨‍💻 Autor

Desarrollado como proyecto de replicación MongoDB con dataset de e-commerce brasileño.

## 📞 Soporte

Si tienes problemas o preguntas:

1. Revisar la sección de "Solución de Problemas"
2. Verificar que todos los prerrequisitos estén instalados
3. Asegurar que Docker esté funcionando correctamente
4. Revisar los logs de Docker: `docker-compose logs`

---

## 🎉 ¡Listo para Usar!

Tu cluster MongoDB con replicación está listo para análisis de datos de e-commerce. ¡Disfruta explorando los datos! 🚀 