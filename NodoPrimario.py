import json
import socket
import threading
import time
import os

from SimpleStorage import AlmacenamientoSimple


class NodoPrimario:
    def __init__(self, host, puerto, direcciones_replicas):
        self.host = host
        self.puerto = puerto
        self.direcciones_replicas = direcciones_replicas
        self.almacenamiento = AlmacenamientoSimple(f"datos_primario.json")

        self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_servidor.bind((self.host, self.puerto))
        self.socket_servidor.listen(5)
        print(f"Nodo primario escuchando en {self.host}:{self.puerto}")

        self.conexiones_replicas = {}
        self._conectar_a_replicas()

        self.ejecutandose = True
        threading.Thread(target=self._aceptar_clientes, daemon=True).start()
        threading.Thread(target=self._enviar_latidos, daemon=True).start()

    def _conectar_a_replicas(self):

        for direccion_replica in self.direcciones_replicas:
            try:
                conexion = socket.create_connection(direccion_replica)
                self.conexiones_replicas[direccion_replica] = conexion
                print(f"Connected to replica {direccion_replica}")
            except Exception as e:
                print(f"No se pudo conectar a la réplica {direccion_replica}: {e}")

    def _aceptar_clientes(self):
        while self.ejecutandose:
            try:
                socket_cliente, direccion_cliente = self.socket_servidor.accept()
                print(f"Conexión aceptada desde el cliente {direccion_cliente}")
                threading.Thread(target=self._manejar_cliente, args=(socket_cliente,), daemon=True).start()
            except Exception as e:
                if self.ejecutandose:
                    print(f"Error al aceptar la conexión del cliente: {e}")
                break

    def _manejar_cliente(self, socket_cliente):
        try:
            mensaje = socket_cliente.recv(4096).decode('utf-8')
            solicitud = json.loads(mensaje)

            operacion = solicitud.get("operacion")
            clave = solicitud.get("clave")
            valor = solicitud.get("valor")

            respuesta = {"estado": "error", "mensaje": "Operación inválid"}

            if operacion == "escribir":
                self.almacenamiento.escribir(clave, valor)
                self._replicar_escritura(clave, valor)
                respuesta = {"estado": "exito", "mensaje": f"Escrito {clave}={valor}"}
            elif operacion == "leer":
                datos = self.almacenamiento.leer(clave)
                respuesta = {"estado": "exito", "valor": datos}
            else:
                print(f"Operación desconocida: {operacion}")

            socket_cliente.sendall(json.dumps(respuesta).encode('utf-8'))

        except Exception as e:
            print(f"Operación desconocida: {e}")
        finally:
            socket_cliente.close()

    def _replicar_escritura(self, clave, valor):
        mensaje_replicacion = json.dumps({"operacion": "replicar", "clave": clave, "valor": valor}).encode('utf-8')
        for direccion, conexion in list(self.conexiones_replicas.items()):
            try:
                conexion.sendall(mensaje_replicacion)
            except Exception as e:
                print(f"Error al replicar a {direccion}: {e}. Eliminando conexión.")
                conexion.close()
                del self.conexiones_replicas[direccion]

    def _enviar_latidos(self):
        while self.ejecutandose:
            mensaje_latido = json.dumps({"operacion": "latido", "timestamp": time.time()}).encode('utf-8')
            for direccion, conexion in list(self.conexiones_replicas.items()):
                try:
                    conexion.sendall(mensaje_latido)
                except Exception as e:
                    print(f"Error al replicar a {direccion}: {e}. Removing connection.")
                    conexion.close()
                    del self.conexiones_replicas[direccion]
            time.sleep(1)

    def detener(self):
        self.ejecutandose = False
        try:
            self.socket_servidor.shutdown(socket.SHUT_RDWR)
        except OSError as e:
            print(f"Error al apagar el socket del servidor: {e}")
        self.socket_servidor.close()
        for conexion in self.conexiones_replicas.values():
            conexion.close()
        print("Nodo primario detenido.")


if __name__ == "__main__":
    config_replicas = [('127.0.0.1', 8001), ('127.0.0.1', 8002)]
    instancia_nodo_primario = NodoPrimario('127.0.0.1', 8000, config_replicas)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nApagando el nodo primario...")
        instancia_nodo_primario.detener()