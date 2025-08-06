# 🛒 Proyecto: Replicación Primario-Secundario MongoDB con Dataset Brazilian E-commerce

## 📋 Descripción del Proyecto

Este proyecto implementa un sistema completo de **replicación Primario-Secundario en MongoDB** utilizando el dataset de **Brazilian E-commerce de Kaggle**. El proyecto incluye:

- **Análisis Exploratorio de Datos (EDA)** completo
- **Proceso ETL** con limpieza inteligente de datos
- **Replicación MongoDB** con Docker (1 primario + 2 secundarios)
- **15 consultas CRUD** complejas con agregaciones
- **Verificación de replicación** entre nodos

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MongoDB       │    │   MongoDB       │    │   MongoDB       │
│   Primary       │◄──►│   Secondary 1   │◄──►│   Secondary 2   │
│   (Port 27017)  │    │   (Port 27018)  │    │   (Port 27019)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Jupyter       │
                    │   Notebooks     │
                    │   (EDA + CRUD)  │
                    └─────────────────┘
```

## 🚀 Características del Proyecto

- **Replicación MongoDB**: Configuración automática de replica set con 1 primario y 2 secundarios
- **Dataset Real**: Dataset de e-commerce brasileño con 9 archivos CSV interrelacionados
- **Análisis Completo**: EDA detallado y proceso ETL robusto
- **15 Consultas CRUD**: Operaciones complejas de Create, Read, Update, Delete
- **Docker Compose**: Configuración automatizada del cluster
- **Jupyter Notebooks**: Análisis interactivo y documentado
- **Scripts de Automatización**: Setup y inicio automático del proyecto
- **Barras de Progreso**: Interfaz visual con tqdm y colores para mejor experiencia de usuario
- **Utilidades de Progreso**: Módulo dedicado para mostrar progreso en notebooks

## 🚀 Configuración Inicial

### 1. Prerrequisitos

- **Python 3.8+**
- **Docker y Docker Compose**
- **Git**
- **Anaconda o Miniconda** (recomendado)

### 2. Clonar el Repositorio

```bash
git clone <URL_DEL_REPOSITORIO>
cd MongoDB_Replicacion_Proyecto
```

### 3. Configurar Entorno Conda

```bash
# Crear entorno conda
conda create -n mongo python=3.8 -y

# Activar entorno
conda activate mongo

# Verificar activación
conda info --envs
```

### 4. Instalar Dependencias

```bash
# Instalar librerías principales
pip install pandas numpy matplotlib seaborn pymongo kagglehub

# O instalar todas las dependencias de una vez
pip install -r requirements.txt
```

### 5. Configurar Kaggle (Opcional)

Si quieres usar tu propia cuenta de Kaggle:

```bash
# Crear archivo kaggle.json en data/
echo '{"username":"tu_usuario","key":"tu_api_key"}' > data/kaggle.json
```

## 📦 Estructura del Proyecto

```
MongoDB_Replicacion_Proyecto/
├── 📁 data/
│   ├── kaggle.json                 # Credenciales Kaggle
│   └── ventas.json                 # Dataset (se descarga automáticamente)
├── 📁 docker/
│   ├── docker-compose.yml          # Configuración MongoDB replica set
│   └── initReplica.js              # Script inicialización replica set
├── 📁 notebooks/
│   ├── EDA_ETL_MongoDB.ipynb       # Análisis exploratorio y ETL
│   └── Consultas_CRUD.ipynb        # 15 consultas CRUD complejas
├── 📁 scripts/
│   ├── start_project.py            # Script de inicio del proyecto (con barras de progreso)
│   ├── setup_project.py            # Script de configuración inicial
│   ├── progress_utils.py           # Utilidades de barras de progreso
│   └── common_utils.py             # Utilidades comunes para scripts
├── 📄 README.md                    # Este archivo
├── 📄 requirements.txt             # Dependencias Python
└── 📄 .gitignore                   # Archivos a ignorar
```

## 🐳 Iniciar el Cluster MongoDB

### Opción 1: Configuración Inicial (Primera vez)

```bash
# Configurar el proyecto por primera vez
python scripts/setup_project.py
```

### Opción 2: Usar el Script Automático (Recomendado)

```bash
# Activar entorno conda
conda activate mongo

# Ejecutar script de inicio (con barras de progreso y colores)
python scripts/start_project.py
```

El script automático incluye:
- ✅ **Barras de progreso** visuales para cada paso
- ✅ **Verificación automática** de requisitos (Docker, Python packages)
- ✅ **Colores** para mejor experiencia de usuario
- ✅ **Animaciones** de progreso
- ✅ **Guía interactiva** para el usuario

### Opción 2: Inicio Manual

```bash
# Navegar al directorio docker
cd docker

# Iniciar cluster MongoDB
docker-compose up -d

# Verificar que los contenedores estén corriendo
docker-compose ps

# Ver logs si hay problemas
docker-compose logs
```

### Verificar Replicación

```bash
# Conectar al primario
docker exec -it mongo-primary mongosh --username admin --password password123

# Verificar estado del replica set
rs.status()
```

## 📊 Ejecutar Análisis de Datos

### 1. Iniciar Jupyter Notebook

```bash
# Activar entorno conda
conda activate mongo

# Iniciar Jupyter
jupyter notebook
```

### 2. Configurar Kernel

Al abrir los notebooks, asegúrate de seleccionar el kernel correcto:
- **Kernel** → **Change kernel** → **Python (mongo)**

Si no aparece el kernel, instálalo:

```bash
conda activate mongo
python -m ipykernel install --user --name mongo --display-name "Python (mongo)"
```

### 3. Ejecutar Notebooks

#### Notebook EDA y ETL (`notebooks/EDA_ETL_MongoDB.ipynb`)

Este notebook realiza:
- ✅ **Descarga automática** del dataset Brazilian E-commerce
- ✅ **Análisis exploratorio** de 9 datasets CSV
- ✅ **Limpieza inteligente** basada en el EDA
- ✅ **Combinación de datasets** (Orders + Items + Products + Customers + Sellers + Payments + Reviews)
- ✅ **Carga a MongoDB** con verificación de replicación
- ✅ **Visualizaciones** completas

#### Notebook CRUD (`notebooks/Consultas_CRUD.ipynb`)

Este notebook incluye **15 consultas complejas**:
1. Ventas de últimos 3 meses por cliente
2. Agregación por producto con totales
3. Análisis de stock y tendencias
4. Consultas desde nodos secundarios
5. Actualizaciones condicionales
6. Eliminaciones con validaciones
7. Agregaciones complejas
8. Optimizaciones con índices

## 🎨 Utilidades de Progreso

### Usar Barras de Progreso en Notebooks

Para mejorar la experiencia visual en los notebooks, puedes importar las utilidades de progreso:

```python
# En tu notebook
from scripts.progress_utils import *

# Mostrar pasos del ETL
progress_etl_steps()

# Mostrar progreso de limpieza de datos
progress_data_cleaning(df)

# Mostrar progreso de operaciones MongoDB
progress_mongodb_operations(["Conectar", "Insertar", "Verificar"])

# Verificar replicación con animación
show_replication_status()

# Mostrar información del DataFrame con progreso
show_dataframe_info_with_progress(df, "Mi Dataset")
```

### Funciones Disponibles

- `progress_etl_steps()`: Muestra los pasos del proceso ETL
- `show_dataframe_info_with_progress()`: Análisis de DataFrame con barra de progreso
- `progress_data_cleaning()`: Progreso de limpieza de datos
- `progress_mongodb_operations()`: Operaciones MongoDB con progreso
- `show_replication_status()`: Verificación de replicación con animación
- `show_dataset_download_progress()`: Progreso de descarga de dataset
- `show_csv_loading_progress()`: Carga de archivos CSV con progreso
- `show_eda_progress()`: Progreso del análisis exploratorio

## 🔧 Comandos Útiles

### Docker

```bash
# Iniciar cluster
docker-compose -f docker/docker-compose.yml up -d

# Detener cluster
docker-compose -f docker/docker-compose.yml down

# Ver logs
docker-compose -f docker/docker-compose.yml logs -f

# Reiniciar servicios
docker-compose -f docker/docker-compose.yml restart
```

### MongoDB

```bash
# Conectar al primario
docker exec -it mongo-primary mongosh --username admin --password password123

# Conectar a secundario 1
docker exec -it mongo-secondary1 mongosh --username admin --password password123

# Conectar a secundario 2
docker exec -it mongo-secondary2 mongosh --username admin --password password123
```

### Python/Anaconda

```bash
# Configuración inicial (primera vez)
python scripts/setup_project.py

# Activar entorno
conda activate mongo

# Verificar librerías
python -c "import pandas, numpy, matplotlib, seaborn, pymongo, kagglehub; print('✅ Todas las librerías funcionan')"

# Instalar kernel Jupyter
python -m ipykernel install --user --name mongo --display-name "Python (mongo)"
```

## 📈 Características del Dataset

### Datasets Originales (9 archivos CSV):
- **olist_customers_dataset**: 99,441 clientes
- **olist_geolocation_dataset**: 1,000,163 ubicaciones
- **olist_orders_dataset**: 99,441 pedidos
- **olist_order_items_dataset**: 112,650 items
- **olist_order_payments_dataset**: 103,886 pagos
- **olist_order_reviews_dataset**: 99,224 reseñas
- **olist_products_dataset**: 32,951 productos
- **olist_sellers_dataset**: 3,095 vendedores
- **product_category_name_translation**: 71 categorías

### Dataset Final Procesado:
- **Registros**: ~100,000 ventas completas
- **Columnas**: 20+ campos relevantes
- **Período**: 2017-2018
- **Geografía**: Brasil completo

## 🎯 Resultados Esperados

### EDA y ETL:
- ✅ Dataset descargado automáticamente
- ✅ Análisis exploratorio completo
- ✅ Limpieza inteligente de datos
- ✅ Dataset unificado cargado en MongoDB
- ✅ Verificación de replicación exitosa

### Consultas CRUD:
- ✅ 15 consultas complejas ejecutadas
- ✅ Operaciones de agregación optimizadas
- ✅ Lecturas desde nodos secundarios
- ✅ Actualizaciones y eliminaciones seguras
- ✅ Análisis de rendimiento con índices

## 🐛 Solución de Problemas

### Error: "ModuleNotFoundError: No module named 'seaborn'"
```bash
conda activate mongo
pip install seaborn
```

### Error: "Connection refused" en MongoDB
```bash
# Verificar que Docker esté corriendo
docker ps

# Reiniciar cluster
docker-compose -f docker/docker-compose.yml down
docker-compose -f docker/docker-compose.yml up -d
```

### Error: Kernel no encontrado en Jupyter
```bash
conda activate mongo
python -m ipykernel install --user --name mongo --display-name "Python (mongo)"
```

### Error: Dataset no se descarga
```bash
# Verificar kagglehub
pip install kagglehub

# O usar datos de ejemplo (el notebook tiene fallback)
```

## 📝 Notas Importantes

1. **Entorno Conda**: Siempre activa el entorno `mongo` antes de trabajar
2. **Puertos**: MongoDB usa puertos 27017, 27018, 27019
3. **Credenciales**: admin/password123 (configurables en docker-compose.yml)
4. **Datos**: El dataset se descarga automáticamente (~100MB)
5. **Replicación**: Espera 2-3 segundos para que se repliquen los datos

## 👥 Contribución

Para contribuir al proyecto:

1. Fork el repositorio
2. Crea una rama para tu feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crea un Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

## 🤝 Autores

- **Tu Nombre** - *Trabajo inicial* - [TuUsuario](https://github.com/TuUsuario)

## 🙏 Agradecimientos

- Dataset: [Brazilian E-commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- MongoDB para la documentación de replicación
- Docker para la containerización

---

**¡Disfruta explorando los datos de e-commerce brasileño con MongoDB! 🚀** 