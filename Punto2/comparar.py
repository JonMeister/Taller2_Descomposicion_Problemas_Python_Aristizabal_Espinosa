#!/usr/bin/env python3
"""Comparación de rendimiento entre procesamiento secuencial y pipeline concurrente.

Este módulo ejecuta pruebas comparativas y benchmarks de rendimiento para evaluar
la aceleración (Speedup) y la integridad de datos entre la versión secuencial
y el pipeline concurrente de 4 etapas.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import sys
import time
from typing import Sequence

from pipeline import procesar_texto_pipeline
from secuencial import procesar_texto_secuencial


def contar_lineas(ruta_archivo: str) -> int:
    """Cuenta el número de líneas de un archivo leyendo bloques binarios de 1 MB.

    Args:
        ruta_archivo: Ruta del archivo a contabilizar.

    Returns:
        int: Total de saltos de línea encontrados en el archivo.
    """
    with open(ruta_archivo, "rb") as f:
        return sum(
            chunk.count(b"\n")
            for chunk in iter(lambda: f.read(1024 * 1024), b"")
        )


def verificar_integridad(archivo1: str, archivo2: str) -> bool:
    """Compara si dos archivos son exactamente idénticos byte a byte.

    Args:
        archivo1: Ruta del primer archivo.
        archivo2: Ruta del segundo archivo.

    Returns:
        bool: True si ambos archivos son idénticos byte a byte, False de lo contrario.
    """
    with open(archivo1, "rb") as f1, open(archivo2, "rb") as f2:
        while True:
            b1 = f1.read(1024 * 1024)
            b2 = f2.read(1024 * 1024)
            if b1 != b2:
                return False
            if not b1:
                return True


def preparar_muestra_archivo(
    ruta_base: str,
    ruta_temp: str,
    num_lineas_objetivo: int,
) -> tuple[str, bool]:
    """Obtiene una ruta de archivo con la cantidad exacta de líneas requerida.

    Si el archivo base ya contiene exactamente el número de líneas, se retorna
    directamente. Si contiene más líneas, extrae las primeras líneas al archivo
    temporal. Si no existe o tiene menos líneas, genera sintéticamente el contenido.

    Args:
        ruta_base: Archivo fuente de referencia.
        ruta_temp: Ruta de destino temporal para escribir la muestra si se requiere.
        num_lineas_objetivo: Número exacto de líneas a procesar.

    Returns:
        tuple[str, bool]: Tupla con la ruta del archivo a usar y un booleano indicando
            si se trata de un archivo temporal que debe eliminarse.
    """
    if os.path.exists(ruta_base):
        total_base = contar_lineas(ruta_base)
        if total_base == num_lineas_objetivo:
            return ruta_base, False
        if total_base > num_lineas_objetivo:
            with open(ruta_base, "r", encoding="utf-8") as f_in, open(
                ruta_temp, "w", encoding="utf-8"
            ) as f_out:
                for i, linea in enumerate(f_in):
                    if i >= num_lineas_objetivo:
                        break
                    f_out.write(linea)
            return ruta_temp, True

    # Generación sintética en caso de que no exista ruta_base o sea insuficiente
    from crear_muestra import crear_archivo_prueba

    crear_archivo_prueba(ruta_temp, num_lineas_objetivo)
    return ruta_temp, True


def ejecutar_benchmark(
    ruta_entrada: str = "texto_entrada.txt",
    tam_lote: int = 500,
) -> None:
    """Ejecuta un benchmark puntual comparando la versión secuencial contra el pipeline.

    Args:
        ruta_entrada: Ruta del archivo de texto fuente.
        tam_lote: Tamaño de lote (líneas) a evaluar en el pipeline concurrente.
    """
    salida_sec = "texto_salida_secuencial.txt"
    salida_pipe = "texto_salida_pipeline.txt"

    num_lineas = contar_lineas(ruta_entrada)

    # Medición secuencial
    t0_sec = time.time()
    procesar_texto_secuencial(ruta_entrada, salida_sec)
    t_sec = time.time() - t0_sec

    # Medición pipeline concurrente
    t0_pipe = time.time()
    procesar_texto_pipeline(ruta_entrada, salida_pipe, tam_lote=tam_lote)
    t_pipe = time.time() - t0_pipe

    # Cálculo de métricas
    speedup = t_sec / t_pipe if t_pipe > 0 else 0.0
    son_identicos = verificar_integridad(salida_sec, salida_pipe)

    # Impresión de resultados
    print(f"Número de líneas:      {num_lineas:,}")
    print(f"Tamaño de chunk:       {tam_lote:,} líneas")
    print(f"Tiempo secuencial:     {t_sec:.4f} s")
    print(f"Tiempo pipeline:       {t_pipe:.4f} s")
    print(f"Speedup:               {speedup:.2f}x")
    print(
        f"Integridad de salidas: {'Correcta (archivos idénticos)' if son_identicos else 'Error de integridad'}"
    )


def ejecutar_bateria(
    ruta_base: str = "texto_entrada.txt",
    tam_lote: int = 500,
    tamanos_lineas: Sequence[int] = (
        100_000,
        500_000,
        1_000_000,
        2_000_000,
        4_000_000,
        6_000_000,
    ),
) -> None:
    """Ejecuta una batería de pruebas de escalabilidad con chunk fijo en 500 líneas.

    Evalúa el desempeño secuencial y paralelo a lo largo de un rango de tamaños
    de archivo (desde 100,000 hasta 6,000,000 de líneas) para analizar el
    comportamiento de la aceleración (Speedup) y la eficiencia del pipeline.

    Args:
        ruta_base: Ruta del archivo de texto base con datos fuente.
        tam_lote: Tamaño de lote en líneas a mantener fijo (por defecto 500).
        tamanos_lineas: Secuencia con los números de líneas a evaluar.
    """
    salida_sec = "texto_salida_secuencial.txt"
    salida_pipe = "texto_salida_pipeline.txt"
    archivo_temp = "_temp_entrada_bateria.txt"

    print("=" * 96)
    print("      BATERÍA DE PRUEBAS: ESCALABILIDAD POR TAMAÑO DE ARCHIVO (CHUNK = 500 LÍNEAS)")
    print("=" * 96)
    print(f" Archivo base:            {ruta_base}")
    print(f" Tamaño de chunk fijo:    {tam_lote:,} líneas")
    print(
        f" Rango de líneas:         {min(tamanos_lineas):,} a {max(tamanos_lineas):,} líneas"
    )
    print("-" * 96)
    print(
        f" {'#':<3} | {'Líneas':<12} | {'Tamaño':<11} | {'T. Secuencial':<14} | {'T. Pipeline':<13} | {'Speedup':<9} | {'Integridad'}"
    )
    print("-" * 96)

    resultados: list[tuple[int, float, float, float]] = []

    try:
        for i, n_lineas in enumerate(tamanos_lineas, 1):
            ruta_prueba, es_temp = preparar_muestra_archivo(
                ruta_base, archivo_temp, n_lineas
            )
            tam_mb = os.path.getsize(ruta_prueba) / (1024 * 1024)

            # Medición secuencial
            t0_sec = time.time()
            procesar_texto_secuencial(ruta_prueba, salida_sec)
            t_sec = time.time() - t0_sec

            # Medición pipeline concurrente (chunk fijo en 500)
            t0_pipe = time.time()
            procesar_texto_pipeline(ruta_prueba, salida_pipe, tam_lote=tam_lote)
            t_pipe = time.time() - t0_pipe

            speedup = t_sec / t_pipe if t_pipe > 0 else 0.0
            identicos = verificar_integridad(salida_sec, salida_pipe)
            estado_int = "Correcta" if identicos else "Error"

            resultados.append((n_lineas, tam_mb, t_pipe, speedup))

            print(
                f" {i:<3} | {f'{n_lineas:,}':<12} | {f'{tam_mb:.2f} MB':<11} | "
                f"{t_sec:>12.4f} s | {t_pipe:>11.4f} s | {speedup:>7.2f}x | {estado_int}"
            )

            # Limpiar archivo temporal generado para la iteración actual
            if es_temp and os.path.exists(archivo_temp):
                try:
                    os.remove(archivo_temp)
                except OSError:
                    pass

    finally:
        if os.path.exists(archivo_temp):
            try:
                os.remove(archivo_temp)
            except OSError:
                pass

    print("-" * 96)
    if resultados:
        mejor_n, mejor_mb, mejor_t, mejor_sp = max(resultados, key=lambda x: x[3])
        prom_sp = sum(r[3] for r in resultados) / len(resultados)
        print(" Resumen de escalabilidad:")
        print(f"   - Speedup promedio:  {prom_sp:.2f}x")
        print(
            f"   - Máximo Speedup:    {mejor_sp:.2f}x ({mejor_n:,} líneas ~ {mejor_mb:.2f} MB)"
        )
    print("=" * 96)


if __name__ == "__main__":
    if hasattr(mp, "set_start_method"):
        try:
            mp.set_start_method("fork")
        except RuntimeError:
            pass

    args = sys.argv[1:]
    if "--bateria" in args or "-b" in args:
        rutas = [a for a in args if not a.startswith("-")]
        archivo = rutas[0] if rutas else "texto_entrada.txt"
        ejecutar_bateria(ruta_base=archivo, tam_lote=500)
    else:
        archivo = "texto_entrada.txt"
        chunk = 500
        for a in args:
            if a.isdigit():
                chunk = int(a)
            elif not a.startswith("-"):
                archivo = a
        ejecutar_benchmark(ruta_entrada=archivo, tam_lote=chunk)
