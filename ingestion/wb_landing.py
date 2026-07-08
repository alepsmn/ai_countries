import datetime
import json
import logging
import requests
import time

from random import uniform

import config

logger = logging.getLogger(__name__)

OUTPUT_DIR = config.WB_DIR
BASE_URL = config.WB_BASE_URL

# Mecanismo de reintentos
MAX_RETRIES = 5
REQUEST_TIMEOUT = 10 # segundos maximos a esperar por respuesta
BACKOFF_BASE = 2 # El tiempo esperara: 1s, 2s, 4s, ...

# Codigos HTTP que si merece la pena reintentar
RETRYABLE_STATUS = [429, 500, 502, 503, 504]

wb_aggregates = [
    "AFE","AFW","ARB","CEB","CSS","EAP","EAR","EAS","ECA","ECS",
    "EMU","EUU","FCS","HIC","HPC","IBD","IBT","IDA","IDB","IDX",
    "LAC","LCN","LDC","LIC","LMC","LMY","LTE","MEA","MIC","MNA",
    "NAC","OED","OSS","PRE","PSS","PST","SAS","SSA","SSF","SST",
    "TEA","TEC","TLA","TMN","TSA","TSS","UMC","WLD","XZN"
]

def ejecutar_peticion_wb(indicator, pagination) -> tuple[dict, list]:
    """
    Recibe un indicador (poblacion, GDP, ...) del que se obtendran los campos
    0 y 1 de la respuesta de la API de WB en forma de tupla, para desempaquetarla
    posteriormente
    """
    params = {
        "format": "json", # si no, devuelve xml
        "date": "2019:2024",
        "per_page": 500,
        "page": pagination
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
            data = r.json()
            return data[0], data[1]
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

    # este else se ejecuta si no se produce nunca break en el FOR - todos los intentos fallaron
    else:
        raise RuntimeError(
            "WorldBank no respondió tras %s intentos" %(MAX_RETRIES)
        ) from last_exc


def ejecutar_paginacion_wb(indicator, page=1) -> list[dict]:
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
    while True:
    
        row0, row1 = ejecutar_peticion_wb(indicator, pagination=page)
        # respuesta vacia o no hay campo de datos o el campo de datos vacio
        # no capturado por raise_for_status - propio de WB
        if not row1:
            break

        # si se guarda antes no se comprueba si la pagina estaba vacia u otro error
        rows.extend(row1) # si se usa append seria lista de listas no lista de dicts (registros)

        if page >= row0["pages"]: break
        page += 1
    return rows

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
        data_indic = ejecutar_paginacion_wb(indicator)
        all_data.extend(data_indic)

        # Extraer helper - OA y WB usan la misma estructura para escribir a disco
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / f"datos-{name}-{datetime.date.today().strftime('%Y%m%d')}.json"
        """
        Cada indicador se almacena de forma independiente en disco
        """
        with output_path.open("w", encoding="utf-8") as f:
            for record in data_indic:
                json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info("Done. %s registros → %s", len(all_data), output_path)

if __name__ == "__main__":
    main()