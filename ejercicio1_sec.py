from PIL import Image
import os
import time


def convertir_a_gris(ruta_imagen):
    """Convierte una imagen a escala de grises."""

    try:
        imagen = Image.open(ruta_imagen)
        imagen_gris = imagen.convert("L")  # 'L' representa escala de grises
        nombre_archivo, extension = os.path.splitext(ruta_imagen)
        ruta_gris = nombre_archivo + "_gris" + extension
        imagen_gris.save(ruta_gris)
        print(f"Imagen convertida: {ruta_imagen} -> {ruta_gris}")
    except FileNotFoundError:
        print(f"Error: No se encontró la imagen {ruta_imagen}")
    except Exception as e:
        print(f"Error al procesar {ruta_imagen}: {e}")


def procesar_imagenes_secuencial(lista_imagenes):
    """Procesa una lista de imágenes secuencialmente."""
    for ruta_imagen in lista_imagenes:
        convertir_a_gris(ruta_imagen)


if __name__ == "__main__":
    directorio_imagenes = "imagenes"
    lista_imagenes = [
        os.path.join(directorio_imagenes, f)
        for f in os.listdir(directorio_imagenes)
        if os.path.isfile(os.path.join(directorio_imagenes, f))
    ]
    inicio = time.time()
    procesar_imagenes_secuencial(lista_imagenes)
    fin = time.time()
    print(f"Tiempo total de procesamiento secuencial: {fin - inicio:.2f}segundos")
