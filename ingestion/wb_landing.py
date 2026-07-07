import json
import logging
import requests
import time

from typing import Any
from pathlib import Path
from random import uniform

import config

logger = logging.getLogger(__name__)

OUTPUT_DIR = config.WB_DIR
BASE_URL = config.WB_BASE_URL

# Mecanismo de reintentos
MAX_RETRIES = 5
REQUEST_TIMEOUT = 2 # segundos maximos a esperar por respuesta
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

def ejecutar_peticion_wb(indicator, page=1) -> dict[str, Any]:
    params = {
        "format": "json", # si no, devuelve xml
        "date": "2019:2024",
        "per_page": 500,
        "page": page
    }

    rows = []

    code = indicator
    while True:

        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(BASE_URL + f"/country/all/indicator/{code}", params=params, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()

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
                    attempt + 1, MAX_RETRIES, exc)
        
            if attempt < MAX_RETRIES - 1:   
                wait = BACKOFF_BASE ** attempt + uniform(0, 0.5)
                logger.info("Reintentando en %ss...", wait)
                time.sleep(wait)

            # respuesta vacia o no hay campo de datos o el campo de datos vacio
            # no capturado por raise_for_status - propio de WB
            if not data or len(data) < 2 or data[1] is None:
                break
            
            if page >= data[0]["pages"]: break
            params["page"] += 1

            return rows # a esto habria q filtrar wb_aggregates en otra funcion consecutiva

        raise RuntimeError(
            "WorldBank no respondió tras %s intentos" %(MAX_RETRIES)
        ) from last_exc


def main() -> None:
    indicators = {
        "SP.POP.TOTL": "POPULATION",
        "NY.GDP.MKTP.CD": "GDP_USD",
        "GB.XPD.RSDV.GD.ZS": "RD_GDP_PCT",
        "SP.POP.SCIE.RD.P6": "RESEARCHERS_PER_MILLION",
        "NY.GDP.PCAP.CD": "GDP_PER_CAPITA_USD"
    }
