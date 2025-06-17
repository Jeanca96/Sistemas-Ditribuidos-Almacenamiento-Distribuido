import socket
import json
import time


class Cliente:
    def __init__(self, host_primario, puerto_primario):
        self.host_primario = host_primario
        self.puerto_primario = puerto_primario

    def _enviar_solicitud(self, operacion, clave, valor=None):
        socket_cliente = None
        try:
            socket_cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_cliente.connect((self.host_primario, self.puerto_primario))

            solicitud = {"operacion": operacion, "clave": clave}
            if valor is not None:
                solicitud["valor"] = valor

            socket_cliente.sendall(json.dumps(solicitud).encode('utf-8'))
            respuesta = socket_cliente.recv(4096).decode('utf-8')
            return json.loads(respuesta)
        except ConnectionRefusedError:

            return {"estado": "error",
                    "mensaje": f"Conexión rechazada. El nodo primario no está funcionando en {self.host_primario}:{self.puerto_primario}."}
        except Exception as e:

            return {"estado": "error",
                    "mensaje": f"Error de comunicación con el nodo primario ({self.host_primario}:{self.puerto_primario}): {e}"}
        finally:

            if socket_cliente:
                socket_cliente.close()

    def escribir(self, clave, valor):
        return self._enviar_solicitud("escribir", clave, valor)

    def leer(self, clave):
        return self._enviar_solicitud("leer", clave)


if __name__ == "__main__":
    direccion_nodo_primario = ('127.0.0.1', 8000)
    instancia_cliente = Cliente(direccion_nodo_primario[0], direccion_nodo_primario[1])

    print("--- Escribiendo datos ---")

    print(instancia_cliente.escribir("nombre", "Alicia"))
    time.sleep(0.1)
    print(instancia_cliente.escribir("ciudad", "Nueva York"))
    time.sleep(0.1)
    print(instancia_cliente.escribir("edad", 30))
    time.sleep(0.1)

    print("\n--- Leyendo datos ---")

    print(instancia_cliente.leer("nombre"))
    print(instancia_cliente.leer("ciudad"))
    print(instancia_cliente.leer("edad"))

    print(instancia_cliente.leer("clave_inexistente"))

#Para realizar la prueba 1 comentamos las líneas siguientes
#Para realizar la prueba 2 descomentamos
    print("\n--- Intento de escritura después de una posible caída del primario ---")
    print(instancia_cliente.escribir("nueva_clave", "nuevo_valor_despues_intento_failover"))

# No sé lo que pasará
