#!/usr/bin/env python3
"""
Script para verificar versiones de paquetes y detectar conflictos
MongoDB Replicación Project - Brazilian E-commerce Dataset Analysis
"""

import subprocess
import sys
import platform
from packaging import version

def print_banner():
    """Imprimir banner del proyecto"""
    print("=" * 60)
    print("🔍 VERIFICADOR DE VERSIONES - MONGODB REPLICACIÓN")
    print("=" * 60)
    print("📋 Verificación de compatibilidad de paquetes")
    print("=" * 60)

def check_package_version(package_name):
    """Verificar versión de un paquete específico"""
    try:
        if platform.system() == "Windows":
            check_cmd = f"conda activate mongo && python -c \"import {package_name}; print({package_name}.__version__)\""
        else:
            check_cmd = f"source activate mongo && python -c \"import {package_name}; print({package_name}.__version__)\""
        
        result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            return None
    except Exception as e:
        return None

def compare_versions(current_version, required_version, package_name):
    """Comparar versiones y determinar compatibilidad"""
    try:
        current = version.parse(current_version)
        required = version.parse(required_version)
        
        if current >= required:
            return True, "✅ Compatible"
        else:
            return False, f"❌ Requiere {required_version}+ (tienes {current_version})"
    except Exception as e:
        return False, f"❌ Error comparando versiones: {e}"

def check_python_version():
    """Verificar versión de Python"""
    print("\n🐍 Verificando versión de Python...")
    version_info = sys.version_info
    python_version = f"{version_info.major}.{version_info.minor}.{version_info.micro}"
    
    print(f"📊 Versión actual: {python_version}")
    
    if version_info.major >= 3 and version_info.minor >= 8:
        print("✅ Python 3.8+ - Compatible")
        return True
    else:
        print("❌ Se requiere Python 3.8+")
        return False

def check_all_packages():
    """Verificar todos los paquetes requeridos"""
    print("\n📦 Verificando paquetes requeridos...")
    
    # Definir paquetes requeridos con versiones mínimas
    required_packages = {
        "pandas": "1.5.0",
        "numpy": "1.21.0", 
        "matplotlib": "3.5.0",
        "seaborn": "0.11.0",
        "pymongo": "4.0.0",
        "tqdm": "4.60.0",
        "colorama": "0.4.4",
        "dateutil": "2.8.0",  # python-dateutil se importa como dateutil
        "pytz": "2022.1",
        "notebook": "6.4.0",
        "ipykernel": "6.0.0",
        "kagglehub": "0.2.9"
    }
    
    all_compatible = True
    missing_packages = []
    
    for package, min_version in required_packages.items():
        current_version = check_package_version(package)
        
        if current_version is None:
            print(f"❌ {package} - No instalado")
            missing_packages.append(package)
            all_compatible = False
        else:
            is_compatible, status = compare_versions(current_version, min_version, package)
            print(f"{status} {package} {current_version}")
            
            if not is_compatible:
                all_compatible = False
    
    return all_compatible, missing_packages

def check_potential_conflicts():
    """Verificar posibles conflictos de versiones"""
    print("\n⚠️ Verificando posibles conflictos...")
    
    # Conflictos conocidos
    known_conflicts = [
        ("Python 3.13+", "kagglehub", "0.3.0", "0.2.9"),
        ("Python 3.13+", "jupyter", "1.0.0", "notebook"),
    ]
    
    version_info = sys.version_info
    is_python_313_plus = version_info.major == 3 and version_info.minor >= 13
    
    for condition, package, old_version, new_version in known_conflicts:
        if condition == "Python 3.13+" and is_python_313_plus:
            current_version = check_package_version(package)
            if current_version:
                print(f"⚠️ {package} {current_version} - Considerar actualizar a {new_version} para Python 3.13+")

def show_recommendations():
    """Mostrar recomendaciones basadas en la verificación"""
    print("\n💡 RECOMENDACIONES:")
    print("1. Si hay paquetes faltantes, ejecuta: python scripts/setup_project.py")
    print("2. Si hay versiones incompatibles, actualiza los paquetes")
    print("3. Para Python 3.13+, usa 'notebook' en lugar de 'jupyter'")
    print("4. Para Python 3.13+, usa 'kagglehub>=0.2.9'")

def main():
    """Función principal"""
    print_banner()
    
    # Verificar Python
    python_ok = check_python_version()
    
    # Verificar paquetes
    all_compatible, missing = check_all_packages()
    
    # Verificar conflictos
    check_potential_conflicts()
    
    # Mostrar resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN DE VERIFICACIÓN:")
    print("=" * 60)
    
    if python_ok and all_compatible:
        print("🎉 ¡Todo está listo! El proyecto debería funcionar correctamente.")
    else:
        print("⚠️ Se encontraron algunos problemas:")
        if not python_ok:
            print("   - Versión de Python incompatible")
        if missing:
            print(f"   - Paquetes faltantes: {', '.join(missing)}")
        if not all_compatible:
            print("   - Algunas versiones son incompatibles")
    
    show_recommendations()
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main() 