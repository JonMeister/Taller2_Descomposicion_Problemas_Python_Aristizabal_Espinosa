#!/usr/bin/env python3
"""Procesamiento secuencial de texto como línea base comparativa.

Este módulo implementa la versión monohilo / secuencial para el procesamiento
de texto. Sirve como referencia base para la medición de tiempos y el cálculo
de la aceleración (Speedup) obtenida con el enfoque paralelo por descomposición
funcional.

Flujo de procesamiento:
    1. Lectura de líneas desde el archivo origen de forma secuencial.
    2. Supresión de espacios en blanco al inicio y final (strip).
    3. Conversión de caracteres a mayúsculas (upper).
    4. Escritura de las líneas resultantes en el archivo de destino.
"""

from __future__ import annotations

import sys
import time


def procesar_texto_secuencial(ruta_entrada: str, ruta_salida: str) -> bool:
    """Procesa el archivo de texto línea por línea de manera secuencial.

    Args:
        ruta_entrada: Ruta del archivo de texto fuente.
        ruta_salida: Ruta del archivo donde se guardará el resultado procesado.

    Returns:
        bool: True si el procesamiento finalizó exitosamente, False si ocurrió un
            error de archivo no encontrado o de I/O.
    """
    try:
        with open(ruta_entrada, "r", encoding="utf-8") as f_in, open(
            ruta_salida, "w", encoding="utf-8"
        ) as f_out:
            for linea in f_in:
                linea_limpia = linea.strip()
                linea_mayusculas = linea_limpia.upper()
                f_out.write(linea_mayusculas + "\n")
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {ruta_entrada}", file=sys.stderr)
        return False
    except OSError as err:
        print(f"Error de E/S al procesar secuencialmente: {err}", file=sys.stderr)
        return False
    return True


if __name__ == "__main__":
    ruta_entrada = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "texto_entrada.txt"
    ruta_salida = (
        sys.argv[2]
        if len(sys.argv) > 2 and not sys.argv[2].startswith("-")
        else "texto_salida_secuencial.txt"
    )

    try:
        with open(ruta_entrada, "rb") as f:
            num_lineas = sum(
                chunk.count(b"\n")
                for chunk in iter(lambda: f.read(1024 * 1024), b"")
            )
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo '{ruta_entrada}'.", file=sys.stderr)
        sys.exit(1)

    print(f"[*] Iniciando procesamiento secuencial de '{ruta_entrada}' ({num_lineas:,} líneas)...")
    inicio = time.time()
    exito = procesar_texto_secuencial(ruta_entrada, ruta_salida)
    fin = time.time()

    if exito:
        tiempo_total = fin - inicio
        print(f"Número de líneas: {num_lineas:,}")
        print(f"Tiempo total de procesamiento secuencial: {tiempo_total:.4f} segundos")
        print(f"Archivo procesado secuencialmente guardado en {ruta_salida}")
    else:
        print(
            f"Error: El procesamiento secuencial falló para '{ruta_entrada}'.",
            file=sys.stderr,
        )
        sys.exit(1)
