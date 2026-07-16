import datetime
import json
import logging
import requests
import time

from random import uniform
from typing import Any
from pathlib import Path

from . import config

logger = logging.getLogger(__name__)

OUTPUT_DIR = config.WB_DIR
BASE_URL = config.WB_BASE_URL

# Mecanismo de reintentos
MAX_RETRIES = 5
REQUEST_TIMEOUT = 10 # segundos maximos a esperar por respuesta
BACKOFF_BASE = 2 # El tiempo esperara: 1s, 2s, 4s, ...

# Codigos HTTP que si merece la pena reintentar
RETRYABLE_STATUS = [400, 429, 500, 502, 503, 504]

#Se introduce 400 para reintentar porque WB lo usa para notificar fallo de infraestructura
#no para errores de parametros (ej: congestion) abortando el pipeline entero
#4xx - determinista seria falso para esta API concreta

def ejecutar_peticion_wb(indicator:str, page:int) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
    """
    Recibe un indicador (poblacion, GDP, ...) del que se obtendran los campos
    0 y 1 de la respuesta de la API de WB en forma de tupla, para desempaquetarla
    posteriormente
    """
    params: dict[str, str | int] = {
        "format": "json", # si no, devuelve xml
        "date": f"{config.YEAR_START}:{config.YEAR_END}",
        "per_page": 500,
        "page": page
    }

    last_exc: Exception | None = None
    """
    Bucle interno que recorrera las paginas disponibles mediante distintas
    peticiones acorde a la paginacion - incremental
    """
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(BASE_URL + f"/country/all/indicator/{indicator}", params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            # Respuesta muy heterogenea para tipado - Lista de Any
            data: list[Any] = r.json()
            try:
                if page == 1:
                    url: str | None = r.url
                else:
                    url = None
                if len(data) == 2:
                    return data[0], data[1], url
            except Exception as e:
                logger.critical(f"Fallo en el formato de vuelta {data}-{e}")
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRYABLE_STATUS:
                # 4xx - determinista: no reintentar
                logger.error("WorldBank respondio %s (no recuperable): %s", status, exc)
                raise
            last_exc = exc
            logger.warning("WorldBank %s  (intento %s/%s)",
                status, attempt + 1, MAX_RETRIES)
    
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            logger.warning("Fallo de red en %s (intento %s/%s): %s",
                indicator, attempt + 1, MAX_RETRIES, exc)
    
        if attempt < MAX_RETRIES - 1:   
            wait = BACKOFF_BASE ** attempt + uniform(0, 0.5)
            logger.info("Reintentando en %ss...", wait)
            time.sleep(wait)

    # Como no hay break no es necesario else
    raise RuntimeError(
        "WorldBank no respondió tras %s intentos" %(MAX_RETRIES)
    ) from last_exc


def ejecutar_paginacion_wb(indicator:str, page:int=1) -> tuple[list[dict[str, Any]], str]:
    """
    Bucle externo a partir del cual, mediante el indicador proporcionado,
    se haran peticiones necesarias para satisfacer el numero de paginas necesarias
    a recorrer y obtener los datos de todas ellas en una lista de diccionarios - records
    """
    rows = []
    """
    Bucle externo desde el cual un indicator podra producir una respuesta
    de varias paginas 
    """
    ruta_grupo = ""
    while True:
        
        row0, row1, url = ejecutar_peticion_wb(indicator, page=page)
        # respuesta vacia o no hay campo de datos o el campo de datos vacio
        # no capturado por raise_for_status - propio de WB
        if not row1:
            break
        if url:
            ruta_grupo = url
        # si se guarda antes no se comprueba si la pagina estaba vacia u otro error
        rows.extend(row1) # si se usa append seria lista de listas no lista de dicts (registros)

        if page >= row0["pages"]: break
        page += 1
    return rows, ruta_grupo

def guardar_crudo(data: list[dict[str, Any]], name: str) -> Path:
    """
    Persiste en disco los registros de un indicador de World Bank.

    Cada indicador se almacena de forma independiente, en ruta fija (sin
    fecha en el nombre): el loader sabe exactamente qué objeto cargar y
    cada run sobreescribe al anterior.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"datos-{name}.json"

    # NDJSON: un objeto por linea, sin indentar. Es el unico formato JSON
    # que acepta el cargador de BigQuery.
    with output_path.open("w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return output_path

def main() -> None:
    config.configure_logging()

    indicators = {
        "SP.POP.TOTL": "POPULATION",
        "NY.GDP.MKTP.CD": "GDP_USD",
        "GB.XPD.RSDV.GD.ZS": "RD_GDP_PCT",
        "SP.POP.SCIE.RD.P6": "RESEARCHERS_PER_MILLION",
        "NY.GDP.PCAP.CD": "GDP_PER_CAPITA_USD"
    }

    all_data = []

    for indicator, name in indicators.items():
        data_indic, url = ejecutar_paginacion_wb(indicator)
        all_data.extend(data_indic)

        ingested_at = datetime.datetime.now(datetime.UTC).isoformat()
        envoltorios = [
            {
                "source": "worldbank",
                "request_url": url,
                "year": int(data["date"]),   # WB lo trae dentro del registro, como string
                "ingested_at": ingested_at,
                "payload": data
            }

            for data in data_indic
        ] 

        path_guardado = guardar_crudo(envoltorios, name)

    logger.info("Done. %s registros → %s", len(all_data), path_guardado)

if __name__ == "__main__":
    main()
