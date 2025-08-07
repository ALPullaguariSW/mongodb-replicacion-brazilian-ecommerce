#!/usr/bin/env python3
"""
Verificar que los notebooks generados sean válidos
"""

import json
from pathlib import Path

def verificar_notebook(notebook_path):
    """Verificar estructura de notebook"""
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        
        print(f"📓 {notebook_path.name}:")
        print(f"  ✅ Total celdas: {len(nb['cells'])}")
        
        # Contar tipos de celdas
        tipos = {}
        for cell in nb['cells']:
            cell_type = cell['cell_type']
            tipos[cell_type] = tipos.get(cell_type, 0) + 1
        
        print(f"  📋 Tipos de celdas: {tipos}")
        
        # Verificar metadatos
        if 'metadata' in nb and 'kernelspec' in nb['metadata']:
            print(f"  🐍 Kernel: {nb['metadata']['kernelspec']['display_name']}")
        
        # Verificar formato
        if nb.get('nbformat') == 4:
            print(f"  ✅ Formato válido: nbformat {nb['nbformat']}")
        else:
            print(f"  ⚠️ Formato: nbformat {nb.get('nbformat', 'desconocido')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en {notebook_path.name}: {e}")
        return False

if __name__ == "__main__":
    print("🔍 VERIFICANDO NOTEBOOKS GENERADOS")
    print("="*60)
    
    notebooks_path = Path('notebooks')
    if not notebooks_path.exists():
        print("❌ Directorio notebooks no existe")
        exit(1)
    
    notebooks = list(notebooks_path.glob('*.ipynb'))
    if not notebooks:
        print("❌ No se encontraron notebooks")
        exit(1)
    
    todos_validos = True
    for notebook in sorted(notebooks):
        if not verificar_notebook(notebook):
            todos_validos = False
        print()
    
    if todos_validos:
        print("🎉 Todos los notebooks son válidos!")
        print("🚀 Ejecutar: jupyter notebook")
    else:
        print("❌ Algunos notebooks tienen errores")
        exit(1)
