from PIL import Image
import os
import time
import multiprocessing


# Convierte una imagen a escala de grises y guarda una nueva copia
def convertir_a_gris(ruta_imagen):
    try:
        imagen = Image.open(ruta_imagen)
        imagen_gris = imagen.convert("L")  # "L" = escala de grises

        # Separa nombre y extensión para crear el nuevo nombre
        nombre_archivo, extension = os.path.splitext(ruta_imagen)
        ruta_gris = nombre_archivo + "_gris" + extension

        imagen_gris.save(ruta_gris)
        print(f"Imagen convertida: {ruta_imagen} -> {ruta_gris}")

    except FileNotFoundError:
        print(f"Error: No se encontró la imagen {ruta_imagen}")
    except Exception as e:
        print(f"Error al procesar {ruta_imagen}: {e}")


# Cada proceso ejecutará esta función con una parte de las imágenes
def procesar_imagenes(lista_imagenes):
    for ruta_imagen in lista_imagenes:
        convertir_a_gris(ruta_imagen)


if __name__ == "__main__":

    directorio_imagenes = "Punto1/imagenes"

    # Obtiene la cantidad de núcleos de CPU disponibles
    num_procesos = multiprocessing.cpu_count()

    procesos = []

    # Obtiene todas las imágenes/archivos del directorio
    lista_imagenes = [
        os.path.join(directorio_imagenes, f)
        for f in os.listdir(directorio_imagenes)
        if os.path.isfile(os.path.join(directorio_imagenes, f))
    ]

    inicio = time.time()

    # Crea varios procesos y reparte las imágenes entre ellos
    for i in range(num_procesos):

        # Calcula qué parte de la lista le corresponde a este proceso
        inicio_porcion = (i * len(lista_imagenes)) // num_procesos

        fin_porcion = (
            (i + 1) * len(lista_imagenes) // num_procesos
            if i < num_procesos - 1
            else len(lista_imagenes)
        )

        # Obtiene la parte de la lista correspondiente
        porcion = lista_imagenes[inicio_porcion:fin_porcion]

        # Crea un proceso que ejecutará procesar_imagenes()
        p = multiprocessing.Process(target=procesar_imagenes, args=(porcion,))

        procesos.append(p)

        # Inicia el proceso
        p.start()

    # Espera a que todos los procesos terminen
    for p in procesos:
        p.join()

    fin = time.time()

    # Calcula el tiempo total de ejecución
    tiempo_total = fin - inicio

    print("Tiempo de ejecución:", tiempo_total, "segundos")
