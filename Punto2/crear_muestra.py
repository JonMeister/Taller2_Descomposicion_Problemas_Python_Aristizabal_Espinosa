#!/usr/bin/env python3
"""
crear_muestra.py
----------------
Genera archivos de texto de prueba para el Ejercicio 2 (Pipeline de Texto).
Permite generar:
  - Una muestra pequeña de prueba rápida (como la descrita en la guía).
  - Una muestra grande (por defecto 1,500,000 líneas ~ 100 MB) para evidenciar
    la aceleración (speedup) del procesamiento paralelo respecto al secuencial.
"""

import os
import sys
import time
import argparse

LINEAS_BASE = [
    "   esto es una linea con espacios al inicio y final.   \n",
    "otra linea en minusculas para transformacion en el pipeline\n",
    "   Y OTRA CON ESPACIOS Y MAYUSCULAS Y MINUSCULAS mezcladas   \n",
    "   Universidad del Valle - Infraestructuras Paralelas y Distribuidas 2026   \n",
    "procesamiento de datos a gran escala mediante descomposicion funcional.\n",
    "    optimizacion de pipelines concurrentes en sistemas multinucleo    \n",
    "la aceleracion depende de minimizar el costo de comunicacion entre tareas\n",
    "   final del bloque de pruebas con transformaciones de texto   \n"
]

def crear_archivo_prueba(ruta_archivo="texto_entrada.txt", num_lineas=1500000):
    """
    Genera un archivo de texto con el número de líneas especificado.
    """
    print(f"[*] Generando archivo '{ruta_archivo}' con {num_lineas:,} líneas...")
    t0 = time.time()
    
    n_base = len(LINEAS_BASE)
    repeticiones = (num_lineas // n_base) + 1
    bloque = "".join(LINEAS_BASE)
    
    # Escribir en bloques de memoria para maximizar velocidad de generación
    lineas_escritas = 0
    with open(ruta_archivo, "w", encoding="utf-8") as f:
        # Escribimos en paquetes de 5,000 bloques (~40,000 líneas por escritura)
        paquete_tam = 5000
        paquete = bloque * paquete_tam
        paquete_lineas = n_base * paquete_tam
        
        while lineas_escritas + paquete_lineas <= num_lineas:
            f.write(paquete)
            lineas_escritas += paquete_lineas
            
        # Completar las líneas restantes
        while lineas_escritas < num_lineas:
            linea = LINEAS_BASE[lineas_escritas % n_base]
            f.write(linea)
            lineas_escritas += 1

    t1 = time.time()
    tam_mb = os.path.getsize(ruta_archivo) / (1024 * 1024)
    print(f"[✓] Archivo '{ruta_archivo}' generado exitosamente:")
    print(f"    - Líneas: {lineas_escritas:,}")
    print(f"    - Tamaño: {tam_mb:.2f} MB")
    print(f"    - Tiempo de generación: {t1 - t0:.2f} segundos\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generador de datos de prueba para Taller 2 - Punto 2")
    parser.add_argument("--archivo", type=str, default="texto_entrada.txt", help="Ruta del archivo de salida")
    parser.add_argument("--lineas", type=int, default=1500000, help="Número de líneas a generar (default: 1,500,000)")
    parser.add_argument("--pequeno", action="store_true", help="Generar muestra mínima para prueba rápida (100 líneas)")
    args = parser.parse_args()

    lineas_a_generar = 100 if args.pequeno else args.lineas
    crear_archivo_prueba(args.archivo, lineas_a_generar)
