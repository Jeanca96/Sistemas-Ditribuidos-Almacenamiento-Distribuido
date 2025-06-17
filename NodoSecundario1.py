import socket
import threading
import time
import json
import os

from SimpleStorage import AlmacenamientoSimple


class NodoSecundario:
    def __init__(self, host, puerto, direccion_primario):
        self.host = host
        self.puerto = puerto
        self.direccion_primario = direccion_primario

        self.almacenamiento = AlmacenamientoSimple(f"datos_secundario_{puerto}.json")
        self.tiempo_ultimo_latido = time.time()
        self.tiempo_espera_latido = 5


        self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_servidor.bind((self.host, self.puerto))
        self.socket_servidor.listen(5)
        print(f"Nodo secundario escuchando en {self.host}:{self.puerto}")

        self.es_primario = False
        self.ejecutandose = True


        threading.Thread(target=self._aceptar_conexiones_primario, daemon=True).start()
        threading.Thread(target=self._monitorear_primario, daemon=True).start()

    def _aceptar_conexiones_primario(self):
        while self.ejecutandose:
            try:
                conexion, direccion = self.socket_servidor.accept()

                if direccion[0] == self.direccion_primario[0]:
                    print(f"Conexión aceptada desde el primario {direccion}")
                    threading.Thread(target=self._manejar_conexion_primario, args=(conexion,), daemon=True).start()
                else:
                    print(f"Conexión de origen desconocido {direccion}. Cerrando.")
                    conexion.close()
            except Exception as e:
                if self.ejecutandose:
                    print(f"Error al aceptar conexión del primario: {e}")
                break

    def _manejar_conexion_primario(self, conexion):
        try:
            while self.ejecutandose:
                mensaje = conexion.recv(4096).decode('utf-8')
                if not mensaje:
                    print("Conexión con el primario cerrada.")
                    break

                solicitud = json.loads(mensaje)
                operacion = solicitud.get("operacion")

                if operacion == "replicar":
                    clave = solicitud.get("clave")
                    valor = solicitud.get("valor")
                    self.almacenamiento.escribir(clave, valor)
                    print(f"[Secundario {self.puerto}] Replicado: {clave} = {valor}")
                elif operacion == "latido":
                    self.tiempo_ultimo_latido = time.time()
                else:
                    print(f"[Secundario {self.puerto}] Operación desconocida del primario: {operacion}")
        except Exception as e:
            print(f"[Secundario {self.puerto}] Error al manejar la conexión del primario: {e}")
        finally:
            conexion.close()

    def _monitorear_primario(self):
        while self.ejecutandose:
            if not self.es_primario and (time.time() - self.tiempo_ultimo_latido) > self.tiempo_espera_latido:
                print(f"\n{'=' * 50}")
                print(f"!!! [Secundario {self.puerto}] ¡El nodo primario ({self.direccion_primario}) está caído! Iniciando la conmutación por error...")
                print(f"{'=' * 50}\n")
                self._promover_a_primario()
                break
            time.sleep(1)

    def _promover_a_primario(self):
        self.es_primario = True
        print(f"*** [Secundario {self.puerto}] ¡Nodo promocionado a NUEVO PRIMARIO! ***")

    def detener(self):
        self.ejecutandose = False
        try:
            self.socket_servidor.shutdown(socket.SHUT_RDWR)
        except OSError as e:
            print(f"Error al apagar el socket: {e}")
        self.socket_servidor.close() # Cerrar el socket principal
        print(f"Nodo secundario {self.puerto} detenido.")


if __name__ == "__main__":
    puerto_nodo = 8002  # PUERTO ESPECÍFICO PARA ESTE NODO SECUNDARIO
    host_primario = '127.0.0.1'
    puerto_primario = 8000

    instancia_nodo_secundario = NodoSecundario(host_primario, puerto_nodo, (host_primario, puerto_primario))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[Secundario {puerto_nodo}] Apagando...")
        instancia_nodo_secundario.detener() # Llamar al método de detención