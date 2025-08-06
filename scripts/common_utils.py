#!/usr/bin/env python3
"""
Utilidades comunes para los scripts del proyecto MongoDB Replicación
"""

import subprocess
import sys

def check_python_version():
    """Verificar versión de Python"""
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        return True, f"Python {version.major}.{version.minor}.{version.micro}"
    else:
        return False, f"Python {version.major}.{version.minor}.{version.micro}"

def check_docker():
    """Verificar si Docker está instalado"""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, "Docker no está instalado o no está en el PATH"
    except FileNotFoundError:
        return False, "Docker no está instalado"

def check_docker_compose():
    """Verificar si Docker Compose está disponible"""
    try:
        result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, "Docker Compose no está disponible"
    except FileNotFoundError:
        return False, "Docker Compose no está instalado"

def check_conda():
    """Verificar si Conda está instalado"""
    try:
        result = subprocess.run(['conda', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, "Conda no está instalado"
    except FileNotFoundError:
        return False, "Conda no está instalado"

def check_python_package(package_name):
    """Verificar si un paquete de Python está instalado"""
    try:
        __import__(package_name)
        return True
    except ImportError:
        return False 