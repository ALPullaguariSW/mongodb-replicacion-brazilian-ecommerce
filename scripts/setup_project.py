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

def check_conda_environment():
    """Verificar si el entorno conda existe"""
    print("\n🔧 Verificando entorno conda 'mongo'...")
    try:
        result = subprocess.run(['conda', 'env', 'list'], capture_output=True, text=True)
        if result.returncode == 0:
            if 'mongo' in result.stdout:
                print("✅ Entorno conda 'mongo' ya existe")
                return True
            else:
                print("❌ Entorno conda 'mongo' no existe")
                return False
        else:
            print(f"❌ Error al verificar entornos: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def create_conda_environment():
    """Crear entorno conda si no existe"""
    if check_conda_environment():
        print("ℹ️ Usando entorno existente 'mongo'")
        return True
    
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

def check_package_version(package_name, min_version=None):
    """Verificar si un paquete está instalado y su versión"""
    try:
        if platform.system() == "Windows":
            check_cmd = f"conda activate mongo && python -c \"import {package_name}; print('{package_name}:' + {package_name}.__version__)\""
        else:
            check_cmd = f"source activate mongo && python -c \"import {package_name}; print('{package_name}:' + {package_name}.__version__)\""
        
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            version = result.stdout.strip().split(':')[1]
            print(f"✅ {package_name} {version} - Ya instalado")
            return True, version
        else:
            print(f"❌ {package_name} - No instalado")
            return False, None
    except Exception as e:
        print(f"❌ Error verificando {package_name}: {e}")
        return False, None

def install_dependencies():
    """Instalar dependencias Python"""
    print("\n📦 Verificando dependencias Python...")
    try:
        # Verificar versión de Python para ajustar dependencias
        version = sys.version_info
        python_version = f"{version.major}.{version.minor}"
        
        print(f"🐍 Versión de Python detectada: {python_version}")
        
        # Definir paquetes requeridos con versiones mínimas
        required_packages = {
            "pandas": "1.5.0",
            "numpy": "1.21.0", 
            "matplotlib": "3.5.0",
            "seaborn": "0.11.0",
            "pymongo": "4.0.0",
            "tqdm": "4.60.0",
            "colorama": "0.4.4",
            "python-dateutil": "2.8.0",
            "pytz": "2022.1"
        }
        
        # Verificar paquetes ya instalados
        missing_packages = []
        for package, min_version in required_packages.items():
            is_installed, current_version = check_package_version(package, min_version)
            if not is_installed:
                missing_packages.append(f"{package}>={min_version}")
        
        if not missing_packages:
            print("✅ Todas las dependencias ya están instaladas")
            return True
        
        print(f"\n📦 Instalando paquetes faltantes: {', '.join(missing_packages)}")
        
        # Manejar Jupyter según la versión de Python
        if version.major == 3 and version.minor >= 13:
            print("⚠️ Python 3.13+ detectado - Instalando Jupyter compatible")
            jupyter_packages = [
                "notebook>=6.4.0",
                "ipykernel>=6.0.0"
            ]
        else:
            jupyter_packages = [
                "jupyter>=1.0.0",
                "ipykernel>=6.0.0"
            ]
        
        # Verificar Jupyter
        for jupyter_pkg in jupyter_packages:
            package_name = jupyter_pkg.split('>=')[0]
            is_installed, _ = check_package_version(package_name)
            if not is_installed:
                missing_packages.append(jupyter_pkg)
        
        # Instalar kagglehub según la versión de Python
        if version.major == 3 and version.minor >= 13:
            print("⚠️ Python 3.13+ detectado - Instalando kagglehub compatible")
            kagglehub_version = "kagglehub>=0.2.9"
        else:
            kagglehub_version = "kagglehub>=0.3.0"
        
        # Verificar kagglehub
        is_installed, _ = check_package_version("kagglehub")
        if not is_installed:
            missing_packages.append(kagglehub_version)
        
        # Instalar paquetes faltantes
        failed_packages = []
        for package in missing_packages:
            print(f"📦 Instalando {package}...")
            if platform.system() == "Windows":
                install_cmd = f"conda activate mongo && pip install {package}"
            else:
                install_cmd = f"source activate mongo && pip install {package}"
            
            result = subprocess.run(install_cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"⚠️ Error al instalar {package}: {result.stderr}")
                failed_packages.append(package)
                # Intentar instalación sin restricciones de versión
                print(f"🔄 Intentando instalación sin restricciones para {package}...")
                package_name = package.split('>=')[0].split('==')[0]
                if platform.system() == "Windows":
                    install_cmd = f"conda activate mongo && pip install {package_name}"
                else:
                    install_cmd = f"source activate mongo && pip install {package_name}"
                
                result2 = subprocess.run(install_cmd, shell=True, capture_output=True, text=True)
                if result2.returncode == 0:
                    print(f"✅ {package_name} instalado correctamente (sin restricciones)")
                else:
                    print(f"❌ No se pudo instalar {package_name}")
            else:
                print(f"✅ {package} instalado correctamente")
        
        if failed_packages:
            print(f"\n⚠️ Paquetes con problemas: {', '.join(failed_packages)}")
            print("💡 Algunos paquetes pueden no estar disponibles para tu versión de Python")
            print("💡 El proyecto puede funcionar con versiones alternativas")
        
        print("✅ Instalación de dependencias completada")
        return True
        
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