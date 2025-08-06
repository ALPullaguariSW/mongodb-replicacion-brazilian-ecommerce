#!/usr/bin/env python3
"""
Script de inicio para el proyecto de Replicación MongoDB
Caso de Estudio: Implementación de Replicación Primario-Secundario en MongoDB
"""

import os
import sys
import subprocess
import time

def print_banner():
    """Imprimir banner del proyecto"""
    print("=" * 80)
    print("🚀 PROYECTO: REPLICACIÓN PRIMARIO-SECUNDARIO EN MONGODB")
    print("=" * 80)
    print("📋 Caso de Estudio: Dataset de Ventas de Tienda")
    print("🎯 Objetivos:")
    print("   • EDA y ETL de datos de ventas")
    print("   • Configuración de replicación MongoDB")
    print("   • 15 consultas CRUD complejas")
    print("   • Pruebas de replicación")
    print("=" * 80)

def check_docker():
    """Verificar si Docker está instalado y ejecutándose"""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Docker está instalado")
            return True
        else:
            print("❌ Docker no está instalado")
            return False
    except FileNotFoundError:
        print("❌ Docker no está instalado")
        return False

def check_docker_compose():
    """Verificar si Docker Compose está disponible"""
    try:
        result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Docker Compose está disponible")
            return True
        else:
            print("❌ Docker Compose no está disponible")
            return False
    except FileNotFoundError:
        print("❌ Docker Compose no está disponible")
        return False

def start_mongodb_cluster():
    """Iniciar el cluster de MongoDB con replicación"""
    print("\n🐳 Iniciando cluster de MongoDB...")
    
    # Cambiar al directorio docker
    os.chdir('docker')
    
    try:
        # Detener contenedores existentes
        subprocess.run(['docker-compose', 'down'], capture_output=True)
        print("🔄 Contenedores anteriores detenidos")
        
        # Iniciar nuevos contenedores
        result = subprocess.run(['docker-compose', 'up', '-d'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Cluster de MongoDB iniciado")
            print("📊 Nodos disponibles:")
            print("   • Primario: localhost:27017")
            print("   • Secundario 1: localhost:27018")
            print("   • Secundario 2: localhost:27019")
            return True
        else:
            print("❌ Error al iniciar el cluster")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        # Volver al directorio raíz
        os.chdir('..')

def wait_for_mongodb():
    """Esperar a que MongoDB esté listo"""
    print("\n⏳ Esperando a que MongoDB esté listo...")
    time.sleep(10)
    print("✅ MongoDB debería estar listo")

def check_python_packages():
    """Verificar paquetes de Python necesarios"""
    required_packages = [
        'pandas', 'numpy', 'matplotlib', 'seaborn', 
        'pymongo', 'jupyter', 'kaggle'
    ]
    
    print("\n📦 Verificando paquetes de Python...")
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - No instalado")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n⚠️ Paquetes faltantes: {', '.join(missing_packages)}")
        print("💡 Instalar con: pip install " + " ".join(missing_packages))
        return False
    else:
        print("✅ Todos los paquetes están instalados")
        return True

def show_next_steps():
    """Mostrar los próximos pasos"""
    print("\n" + "=" * 80)
    print("🎯 PRÓXIMOS PASOS")
    print("=" * 80)
    print("1. 📊 Ejecutar EDA y ETL:")
    print("   jupyter notebook notebooks/EDA_ETL_MongoDB.ipynb")
    print()
    print("2. 🔍 Ejecutar Consultas CRUD:")
    print("   jupyter notebook notebooks/Consultas_CRUD.ipynb")
    print()
    print("3. 🐳 Verificar estado del cluster:")
    print("   docker-compose -f docker/docker-compose.yml ps")
    print()
    print("4. 📊 Conectar a MongoDB:")
    print("   mongodb://admin:password123@localhost:27017/")
    print()
    print("5. 🛑 Detener cluster:")
    print("   docker-compose -f docker/docker-compose.yml down")
    print("=" * 80)

def main():
    """Función principal"""
    print_banner()
    
    # Verificar requisitos
    print("\n🔍 Verificando requisitos del sistema...")
    
    if not check_docker():
        print("❌ Docker es requerido para este proyecto")
        return
    
    if not check_docker_compose():
        print("❌ Docker Compose es requerido para este proyecto")
        return
    
    if not check_python_packages():
        print("⚠️ Algunos paquetes de Python faltan")
        print("💡 Continúa con la instalación manual si es necesario")
    
    # Preguntar si iniciar el cluster
    print("\n🤔 ¿Deseas iniciar el cluster de MongoDB ahora? (s/n): ", end="")
    response = input().lower().strip()
    
    if response in ['s', 'si', 'sí', 'y', 'yes']:
        if start_mongodb_cluster():
            wait_for_mongodb()
            print("\n🎉 ¡Proyecto listo para usar!")
        else:
            print("\n❌ Error al iniciar el cluster")
            print("💡 Verifica los logs de Docker")
    else:
        print("\nℹ️ Cluster no iniciado")
        print("💡 Puedes iniciarlo manualmente con: docker-compose -f docker/docker-compose.yml up -d")
    
    show_next_steps()

if __name__ == "__main__":
    main() 