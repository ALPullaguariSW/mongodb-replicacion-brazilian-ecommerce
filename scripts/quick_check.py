#!/usr/bin/env python3
"""
Verificación rápida de paquetes del proyecto MongoDB Replicación
"""

import sys

def check_packages():
    """Verificar paquetes requeridos"""
    print("🔍 Verificando paquetes del proyecto MongoDB Replicación...")
    print("=" * 60)
    
    packages = [
        ("pandas", "1.5.0"),
        ("numpy", "1.21.0"),
        ("matplotlib", "3.5.0"),
        ("seaborn", "0.11.0"),
        ("pymongo", "4.0.0"),
        ("tqdm", "4.60.0"),
        ("colorama", "0.4.4"),
        ("dateutil", "2.8.0"),
        ("pytz", "2022.1"),
        ("notebook", "6.4.0"),
        ("ipykernel", "6.0.0"),
        ("kagglehub", "0.2.9")
    ]
    
    all_ok = True
    missing = []
    
    for package, min_version in packages:
        try:
            module = __import__(package)
            version = getattr(module, '__version__', 'unknown')
            print(f"✅ {package} {version}")
        except ImportError:
            print(f"❌ {package} - No instalado")
            missing.append(package)
            all_ok = False
    
    print("\n" + "=" * 60)
    if all_ok:
        print("🎉 ¡Todos los paquetes están instalados!")
    else:
        print(f"⚠️ Paquetes faltantes: {', '.join(missing)}")
        print("💡 Ejecuta: python scripts/setup_project.py")
    
    print(f"🐍 Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")

if __name__ == "__main__":
    check_packages() 