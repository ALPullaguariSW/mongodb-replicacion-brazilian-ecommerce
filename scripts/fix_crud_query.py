#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para corregir la consulta problemática en Consultas_CRUD.ipynb
"""

import json
import re

def fix_crud_notebook():
    """Corregir la consulta problemática en el notebook CRUD"""
    
    print("🔧 Corrigiendo consulta problemática en Consultas_CRUD.ipynb...")
    
    # Leer el notebook
    with open('notebooks/Consultas_CRUD.ipynb', 'r', encoding='utf-8') as f:
        notebook = json.load(f)
    
    # Buscar la celda con la consulta problemática
    for cell in notebook['cells']:
        if cell['cell_type'] == 'code':
            source = ''.join(cell['source'])
            
            # Buscar la consulta problemática
            if 'Consulta 1: Ventas de los últimos 3 meses por cliente' in source:
                print("📝 Encontrada celda con consulta problemática")
                
                # Corregir la consulta
                corrected_source = []
                for line in cell['source']:
                    # Corregir la línea problemática
                    if "cliente_ejemplo = collection.find_one({}, {'id_cliente_unico':'3ce436f183e68e07877b285a838db11a'})['id_cliente_unico']" in line:
                        corrected_line = "# Cliente específico (usando un cliente que existe en el dataset)\n"
                        corrected_source.append(corrected_line)
                        corrected_line = "cliente_ejemplo = '0000366f3b9a7992bf8c76cfdf3221e2'  # Cliente que existe en el dataset\n"
                        corrected_source.append(corrected_line)
                    elif "fecha_limite = \"2017-06-01\"" in line:
                        corrected_line = "# Fecha de hace 3 meses (ajustada para el dataset)\n"
                        corrected_source.append(corrected_line)
                        corrected_line = "fecha_limite = \"2018-02-01\"  # 3 meses antes del final del dataset (2018-05)\n"
                        corrected_source.append(corrected_line)
                    else:
                        corrected_source.append(line)
                
                cell['source'] = corrected_source
                print("✅ Consulta corregida")
                break
    
    # Guardar el notebook corregido
    with open('notebooks/Consultas_CRUD.ipynb', 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)
    
    print("✅ Notebook Consultas_CRUD.ipynb corregido")

if __name__ == "__main__":
    fix_crud_notebook() 