#!/usr/bin/env python3
"""
Script de configuración inicial del proyecto MongoDB Replicación
Brazilian E-commerce Dataset Analysis
"""

import os
import sys
import subprocess
import platform

def print_banner():
    """Imprimir banner del proyecto"""
    print("=" * 60)
    print("🛒 PROYECTO: REPLICACIÓN MONGODB - BRAZILIAN E-COMMERCE")
    print("=" * 60)
    print("📋 Configuración inicial del entorno de desarrollo")
    print("=" * 60)

def check_python_version():
    """Verificar versión de Python"""
    print("\n🐍 Verificando versión de Python...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Se requiere Python 3.8+")
        return False

def check_docker():
    """Verificar si Docker está instalado"""
    print("\n🐳 Verificando Docker...")
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker instalado: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker no está instalado o no está en el PATH")
            return False
    except FileNotFoundError:
        print("❌ Docker no está instalado")
        return False

def check_docker_compose():
    """Verificar si Docker Compose está disponible"""
    print("\n📦 Verificando Docker Compose...")
    try:
        result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker Compose disponible: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker Compose no está disponible")
            return False
    except FileNotFoundError:
        print("❌ Docker Compose no está instalado")
        return False

def check_conda():
    """Verificar si Conda está instalado"""
    print("\n📚 Verificando Conda...")
    try:
        result = subprocess.run(['conda', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Conda instalado: {result.stdout.strip()}")
            return True
        else:
            print("❌ Conda no está instalado")
            return False
    except FileNotFoundError:
        print("❌ Conda no está instalado")
        return False

def create_conda_environment():
    """Crear entorno conda"""
    print("\n🔧 Creando entorno conda 'mongo'...")
    try:
        # Crear entorno
        result = subprocess.run(['conda', 'create', '-n', 'mongo', 'python=3.8', '-y'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Entorno conda 'mongo' creado exitosamente")
            return True
        else:
            print(f"❌ Error al crear entorno: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def install_dependencies():
    """Instalar dependencias Python"""
    print("\n📦 Instalando dependencias Python...")
    try:
        # Activar entorno y instalar dependencias
        if platform.system() == "Windows":
            activate_cmd = "conda activate mongo && pip install -r requirements.txt"
            result = subprocess.run(activate_cmd, shell=True, capture_output=True, text=True)
        else:
            activate_cmd = "source activate mongo && pip install -r requirements.txt"
            result = subprocess.run(activate_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Dependencias instaladas exitosamente")
            return True
        else:
            print(f"❌ Error al instalar dependencias: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def setup_jupyter_kernel():
    """Configurar kernel de Jupyter"""
    print("\n📓 Configurando kernel de Jupyter...")
    try:
        if platform.system() == "Windows":
            kernel_cmd = "conda activate mongo && python -m ipykernel install --user --name mongo --display-name \"Python (mongo)\""
            result = subprocess.run(kernel_cmd, shell=True, capture_output=True, text=True)
        else:
            kernel_cmd = "source activate mongo && python -m ipykernel install --user --name mongo --display-name \"Python (mongo)\""
            result = subprocess.run(kernel_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Kernel de Jupyter configurado")
            return True
        else:
            print(f"❌ Error al configurar kernel: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def create_directories():
    """Crear directorios necesarios"""
    print("\n📁 Creando directorios necesarios...")
    directories = [
        'data',
        'logs',
        'temp'
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✅ Directorio '{directory}' creado")
        else:
            print(f"ℹ️ Directorio '{directory}' ya existe")

def create_sample_kaggle_config():
    """Crear archivo de configuración de Kaggle de ejemplo"""
    print("\n🔑 Creando configuración de Kaggle de ejemplo...")
    kaggle_file = "data/kaggle.json"
    
    if not os.path.exists(kaggle_file):
        sample_config = '''{
    "username": "tu_usuario_kaggle",
    "key": "tu_api_key_kaggle"
}'''
        with open(kaggle_file, 'w') as f:
            f.write(sample_config)
        print("✅ Archivo kaggle.json de ejemplo creado")
        print("💡 Recuerda actualizar con tus credenciales reales de Kaggle")
    else:
        print("ℹ️ Archivo kaggle.json ya existe")

def show_next_steps():
    """Mostrar próximos pasos"""
    print("\n" + "=" * 60)
    print("🎉 ¡CONFIGURACIÓN COMPLETADA!")
    print("=" * 60)
    print("\n📋 PRÓXIMOS PASOS:")
    print("1. Activar entorno conda:")
    print("   conda activate mongo")
    print("\n2. Iniciar cluster MongoDB:")
    print("   docker-compose -f docker/docker-compose.yml up -d")
    print("\n3. Iniciar Jupyter Notebook:")
    print("   jupyter notebook")
    print("\n4. Abrir notebooks:")
    print("   - notebooks/EDA_ETL_MongoDB.ipynb")
    print("   - notebooks/Consultas_CRUD.ipynb")
    print("\n5. Seleccionar kernel 'Python (mongo)' en Jupyter")
    print("\n📚 DOCUMENTACIÓN:")
    print("   - README.md para instrucciones detalladas")
    print("   - scripts/start_project.py para inicio automático")
    print("\n🔧 COMANDOS ÚTILES:")
    print("   - Verificar cluster: docker-compose -f docker/docker-compose.yml ps")
    print("   - Ver logs: docker-compose -f docker/docker-compose.yml logs")
    print("   - Detener cluster: docker-compose -f docker/docker-compose.yml down")
    print("\n" + "=" * 60)

def main():
    """Función principal"""
    print_banner()
    
    # Verificar prerrequisitos
    checks = [
        ("Python 3.8+", check_python_version),
        ("Docker", check_docker),
        ("Docker Compose", check_docker_compose),
        ("Conda", check_conda)
    ]
    
    all_checks_passed = True
    for name, check_func in checks:
        if not check_func():
            all_checks_passed = False
            print(f"⚠️ {name} es requerido para el proyecto")
    
    if not all_checks_passed:
        print("\n❌ Algunos prerrequisitos no están instalados")
        print("💡 Instala los componentes faltantes antes de continuar")
        return
    
    # Configurar proyecto
    setup_steps = [
        ("Crear entorno conda", create_conda_environment),
        ("Instalar dependencias", install_dependencies),
        ("Configurar Jupyter", setup_jupyter_kernel),
        ("Crear directorios", create_directories),
        ("Configurar Kaggle", create_sample_kaggle_config)
    ]
    
    for name, setup_func in setup_steps:
        print(f"\n🔄 {name}...")
        if not setup_func():
            print(f"❌ Error en {name}")
            return
    
    show_next_steps()

if __name__ == "__main__":
    main() 