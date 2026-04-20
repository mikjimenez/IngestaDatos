import os
import shutil
import logging
from datetime import datetime

# Configuración de logging
log_path = "logs/ingesta.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def ingesta_datos(origen, destino):
    try:
        logging.info("Inicio del proceso de ingesta")

        # Crear carpeta destino si no existe
        os.makedirs(os.path.dirname(destino), exist_ok=True)

        # Copiar archivo
        shutil.copy(origen, destino)

        # Contar registros 
        with open(destino, "r", encoding="utf-8") as f:
            lineas = len(f.readlines()) - 1  

        logging.info(f"Ingesta exitosa. Registros procesados: {lineas}")
        print("Archivo copiado correctamente.")

    except Exception as e:
        logging.error(f"Error en la ingesta: {e}")
        print("Error en el proceso.")

if __name__ == "__main__":
    origen = "origen/datos.csv"
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = f"data/raw/datos_{fecha}.csv"

    ingesta_datos(origen, destino)