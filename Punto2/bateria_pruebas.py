#!/usr/bin/env python3
"""Ejecución automatizada de la batería de pruebas de escalabilidad.

Este script ejecuta la batería de pruebas manteniendo fijo el tamaño de chunk
en 500 líneas y evaluando el desempeño del pipeline concurrente frente a la
versión secuencial para distintos volúmenes de datos (desde 100,000 hasta
6,000,000 de líneas).
"""

from __future__ import annotations

import multiprocessing as mp
import sys

from comparar import ejecutar_bateria

if __name__ == "__main__":
    if hasattr(mp, "set_start_method"):
        try:
            mp.set_start_method("fork")
        except RuntimeError:
            pass

    archivo = (
        sys.argv[1]
        if len(sys.argv) > 1 and not sys.argv[1].startswith("-")
        else "texto_entrada.txt"
    )
    ejecutar_bateria(ruta_base=archivo, tam_lote=500)
