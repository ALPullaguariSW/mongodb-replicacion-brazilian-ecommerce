#!/usr/bin/env python3
"""
Script de inicio para el proyecto de Replicación MongoDB
Caso de Estudio: Implementación de Replicación Primario-Secundario en MongoDB
"""

import os
import sys
import subprocess
import time
from tqdm import tqdm
from colorama import init, Fore
from common_utils import check_docker, check_docker_compose, check_python_package

# Inicializar colorama para colores en Windows
init(autoreset=True)

def print_banner():
    """Imprimir banner del proyecto"""
    print(f"{Fore.CYAN}{'=' * 80}")
    print(f"{Fore.YELLOW}🚀 PROYECTO: REPLICACIÓN PRIMARIO-SECUNDARIO EN MONGODB")
    print(f"{Fore.CYAN}{'=' * 80}")
    print(f"{Fore.WHITE}📋 Caso de Estudio: Dataset de Ventas de Tienda")
    print(f"{Fore.WHITE}🎯 Objetivos:")
    print(f"{Fore.GREEN}   • EDA y ETL de datos de ventas")
    print(f"{Fore.GREEN}   • Configuración de replicación MongoDB")
    print(f"{Fore.GREEN}   • 15 consultas CRUD complejas")
    print(f"{Fore.GREEN}   • Pruebas de replicación")
    print(f"{Fore.CYAN}{'=' * 80}")

def check_docker():
    """Verificar si Docker está instalado y ejecutándose"""
    print(f"\n{Fore.BLUE}🔍 Verificando Docker...")
    
    with tqdm(total=1, desc="Docker", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        is_installed, version = check_docker()
        if is_installed:
            pbar.set_description(f"{Fore.GREEN}✅ Docker")
            pbar.update(1)
            print(f"{Fore.GREEN}✅ Docker está instalado: {version}")
            return True
        else:
            pbar.set_description(f"{Fore.RED}❌ Docker")
            pbar.update(1)
            print(f"{Fore.RED}❌ {version}")
            return False

def check_docker_compose():
    """Verificar si Docker Compose está disponible"""
    print(f"\n{Fore.BLUE}🔍 Verificando Docker Compose...")
    
    with tqdm(total=1, desc="Docker Compose", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        is_installed, version = check_docker_compose()
        if is_installed:
            pbar.set_description(f"{Fore.GREEN}✅ Docker Compose")
            pbar.update(1)
            print(f"{Fore.GREEN}✅ Docker Compose está disponible: {version}")
            return True
        else:
            pbar.set_description(f"{Fore.RED}❌ Docker Compose")
            pbar.update(1)
            print(f"{Fore.RED}❌ {version}")
            return False

def start_mongodb_cluster():
    """Iniciar el cluster de MongoDB con replicación"""
    print(f"\n{Fore.YELLOW}🐳 Iniciando cluster de MongoDB...")
    
    # Cambiar al directorio docker
    os.chdir('docker')
    
    try:
        # Detener contenedores existentes
        print(f"{Fore.BLUE}🔄 Deteniendo contenedores anteriores...")
        with tqdm(total=1, desc="Deteniendo contenedores", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
            subprocess.run(['docker-compose', 'down'], capture_output=True)
            pbar.update(1)
        
        # Iniciar nuevos contenedores
        print(f"{Fore.BLUE}🚀 Iniciando nuevos contenedores...")
        with tqdm(total=3, desc="Iniciando cluster", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
            result = subprocess.run(['docker-compose', 'up', '-d'], capture_output=True, text=True)
            pbar.update(3)
            
        if result.returncode == 0:
            print(f"{Fore.GREEN}✅ Cluster de MongoDB iniciado exitosamente")
            print(f"{Fore.CYAN}📊 Nodos disponibles:")
            print(f"{Fore.WHITE}   • Primario: localhost:27017")
            print(f"{Fore.WHITE}   • Secundario 1: localhost:27018")
            print(f"{Fore.WHITE}   • Secundario 2: localhost:27019")
            return True
        else:
            print(f"{Fore.RED}❌ Error al iniciar el cluster")
            print(f"{Fore.RED}{result.stderr}")
            return False
    except Exception as e:
        print(f"{Fore.RED}❌ Error: {e}")
        return False
    finally:
        # Volver al directorio raíz
        os.chdir('..')

def wait_for_mongodb():
    """Esperar a que MongoDB esté listo"""
    print(f"\n{Fore.YELLOW}⏳ Esperando a que MongoDB esté listo...")
    
    with tqdm(total=10, desc="Esperando MongoDB", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        for i in range(10):
            time.sleep(1)
            pbar.update(1)
    
    print(f"{Fore.GREEN}✅ MongoDB debería estar listo")

def check_python_packages():
    """Verificar paquetes de Python necesarios"""
    required_packages = [
        'pandas', 'numpy', 'matplotlib', 'seaborn', 
        'pymongo', 'jupyter', 'kagglehub', 'tqdm', 'colorama'
    ]
    
    print(f"\n{Fore.BLUE}📦 Verificando paquetes de Python...")
    missing_packages = []
    
    with tqdm(total=len(required_packages), desc="Verificando paquetes", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        for package in required_packages:
            if check_python_package(package):
                pbar.set_description(f"{Fore.GREEN}✅ {package}")
                pbar.update(1)
            else:
                pbar.set_description(f"{Fore.RED}❌ {package}")
                pbar.update(1)
                missing_packages.append(package)
    
    if missing_packages:
        print(f"\n{Fore.YELLOW}⚠️ Paquetes faltantes: {', '.join(missing_packages)}")
        print(f"{Fore.CYAN}💡 Instalar con: pip install " + " ".join(missing_packages))
        return False
    else:
        print(f"{Fore.GREEN}✅ Todos los paquetes están instalados")
        return True

def show_progress_animation():
    """Mostrar animación de progreso"""
    print(f"\n{Fore.MAGENTA}🎉 ¡Proyecto iniciado exitosamente!")
    
    with tqdm(total=100, desc="Preparando entorno", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        for i in range(100):
            time.sleep(0.03)
            pbar.update(1)

def show_next_steps():
    """Mostrar los próximos pasos"""
    print(f"\n{Fore.CYAN}{'=' * 80}")
    print(f"{Fore.YELLOW}🎯 PRÓXIMOS PASOS")
    print(f"{Fore.CYAN}{'=' * 80}")
    print(f"{Fore.WHITE}1. 📊 Ejecutar EDA y ETL:")
    print(f"{Fore.GREEN}   jupyter notebook notebooks/EDA_ETL_MongoDB.ipynb")
    print()
    print(f"{Fore.WHITE}2. 🔍 Ejecutar Consultas CRUD:")
    print(f"{Fore.GREEN}   jupyter notebook notebooks/Consultas_CRUD.ipynb")
    print()
    print(f"{Fore.WHITE}3. 🐳 Verificar estado del cluster:")
    print(f"{Fore.GREEN}   docker-compose -f docker/docker-compose.yml ps")
    print()
    print(f"{Fore.WHITE}4. 📊 Conectar a MongoDB:")
    print(f"{Fore.GREEN}   mongodb://admin:password123@localhost:27017/")
    print()
    print(f"{Fore.WHITE}5. 🛑 Detener cluster:")
    print(f"{Fore.GREEN}   docker-compose -f docker/docker-compose.yml down")
    print(f"{Fore.CYAN}{'=' * 80}")

def main():
    """Función principal"""
    print_banner()
    
    # Verificar requisitos
    print(f"\n{Fore.BLUE}🔍 Verificando requisitos del sistema...")
    
    docker_ok = check_docker()
    docker_compose_ok = check_docker_compose()
    packages_ok = check_python_packages()
    
    if not docker_ok:
        print(f"{Fore.RED}❌ Docker es requerido para este proyecto")
        return
    
    if not docker_compose_ok:
        print(f"{Fore.RED}❌ Docker Compose es requerido para este proyecto")
        return
    
    if not packages_ok:
        print(f"{Fore.YELLOW}⚠️ Algunos paquetes de Python faltan")
        print(f"{Fore.CYAN}💡 Continúa con la instalación manual si es necesario")
    
    # Preguntar si iniciar el cluster
    print(f"\n{Fore.YELLOW}🤔 ¿Deseas iniciar el cluster de MongoDB ahora? (s/n): ", end="")
    response = input().lower().strip()
    
    if response in ['s', 'si', 'sí', 'y', 'yes']:
        if start_mongodb_cluster():
            wait_for_mongodb()
            show_progress_animation()
            print(f"\n{Fore.GREEN}🎉 ¡Proyecto listo para usar!")
        else:
            print(f"\n{Fore.RED}❌ Error al iniciar el cluster")
            print(f"{Fore.CYAN}💡 Verifica los logs de Docker")
    else:
        print(f"\n{Fore.BLUE}ℹ️ Cluster no iniciado")
        print(f"{Fore.CYAN}💡 Puedes iniciarlo manualmente con: docker-compose -f docker/docker-compose.yml up -d")
    
    show_next_steps()

if __name__ == "__main__":
    main() 