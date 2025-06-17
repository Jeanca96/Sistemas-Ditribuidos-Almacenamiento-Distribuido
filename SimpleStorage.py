import json
import os


class AlmacenamientoSimple:
    def __init__(self, archivo_datos="datos.json"):
        self.archivo_datos = archivo_datos
        self.datos = self._cargar_datos()

    def _cargar_datos(self):
        if os.path.exists(self.archivo_datos):
            try:
                with open(self.archivo_datos, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(
                    f"Advertencia: El archivo de datos {self.archivo_datos} está vacío o corrupto. Iniciando con datos vacíoss.")
                return {}
        return {}

    def _guardar_datos(self):
        with open(self.archivo_datos, 'w') as f:
            json.dump(self.datos, f, indent=4)

    def escribir(self, clave, valor):
        self.datos[clave] = valor
        self._guardar_datos()
        print(f"[{self.archivo_datos}] Escrito: {clave} = {valor}")

    def leer(self, clave):
        return self.datos.get(clave)

    def eliminar(self, clave):
        if clave in self.datos:
            del self.datos[clave]
            self._guardar_datos()
            print(f"[{self.archivo_datos}] eliminado: {clave}")
            return True
        return False
