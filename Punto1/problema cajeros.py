"""Este problema sucede cuando varios cajeros intentan modificar el saldo de una cuenta a la vez,
cada cajero sera simulado por un hilo y la cuenta sera le memoria compartida"""

import threading
import random

lock = threading.Lock()


def cuenta(saldo_a_retirar):
    global saldo_actual
    # seccion critica:
    with (
        lock
    ):  # Solo 1 cajero puede modificar el saldo a la vez, de lo contrario se perderia la consistencia del éste
        if saldo_actual < 0:  # El saldo de la cuenta no puede ser negativo
            print("Saldo insuficiente")
        else:
            saldo_actual -= saldo_a_retirar


if __name__ == "__main__":
    saldo_actual = 1000000
    n = 3
    # genera n montos a retirar entre 100.000 a 400.000
    montos = [random.randint(1, 4) * 100000 for i in range(1, n + 1)]
    total = sum(montos)
    # cada cajero es un hilo que va a modificar la memoria (saldo de la cuenta)
    cajeros = [
        threading.Thread(target=cuenta, args=(montos[i - 1],))
        for i in range(len(montos))
    ]
    for i, monto in enumerate(montos, 1):
        print(f"Se van a retirar ${monto} en el cajero {i}")
    # Iniciamos todos los cajeros
    for cajero in cajeros:
        cajero.start()
    # Esperamos a que todos terminen
    for cajero in cajeros:
        cajero.join()
    # El saldo que queda en la cuenta despues de los n retiros solo se muestra cuando todos han terminado
    print(f"El nuevo saldo es: ${saldo_actual} despues de retirar un total de {total}")
