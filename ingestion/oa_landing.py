import json
import logging
import requests
import time

from typing import Any
from pathlib import Path

import config

logger = logging.getLogger(__name__)

OUTPUT_DIR = config.OA_DIR
BASE_URL = config.OA_BASE_URL
MAIL_TO = config.USER_MAIL
API_KEY = config.OPENALEX_API_KEY

# Mecanismo de reintentos
MAX_RETRIES = 5
REQUEST_TIMEOUT = 2 # segundos maximos a esperar por respuesta
BACKOFF_BASE = 2 # El tiempo esperara: 1s, 2s, 4s, ...

# Codigos HTTP que si merece la pena reintentar
RETRYABLE_STATUS = [429, 500, 502, 503, 504]

def ejecutar_peticion_oa(id_tarea, api) -> dict[str, Any]:
    """
    Ejecuta la peticion a OA con reintentos y backoff exponencial.
    id_tarea puede ser ano o el offset a procesar
    """
    headers = {
        "User-Agent": f"MiAplicacionIA/1.0 (mailto:{MAIL_TO})"
    }
    params  = {
        "filter": f"topics.subfield.id:1702,publication_year:{id_tarea}",
        "group_by": "authorships.countries",
    }

    if api:
        params["api_key"] = api

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(BASE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRYABLE_STATUS:
                # 4xx - determinista: no reintentar
                logger.error("OpenAlex respondio %s (no recuperable): %s", status, exc)
                raise
            last_exc = exc
            logger.warning("OpenAlex %s en %s (intento %s/%s)",
                status, id_tarea, attempt + 1, MAX_RETRIES)
        
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            logger.warning("Fallo de red en %s (intento %s/%s): %s",
                id_tarea, attempt + 1, MAX_RETRIES, exc)
        
        if attempt < MAX_RETRIES - 1:   
            wait = BACKOFF_BASE ** attempt
            logger.info("Reintentando en %ss...", wait)
            time.sleep(wait)
        
    raise RuntimeError(
        "OpenAlex no respondió tras %s intentos (%s)" %(MAX_RETRIES, id_tarea)
    ) from last_exc # Exception Chaining: muestra el historial completo del desastre - cronologico

def guardar_crudo(datos: dict, id_tarea) -> Path:
    """
    Aterriza la respuesta cruda de OA en OA_DIR (local)
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"openalex_{id_tarea}.json"

    with  output_path.open("w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2) # ensure_ascii=False -> tildes/ñ legibles
    return output_path

# ------------------------------------------------------------------------------------------------------------

def main() -> None:
    config.configure_logging()
    if not API_KEY:
        raise RuntimeError(
            "Falta API_KEY de OpenAlex en el entorno (.env) Revisar"
        )

    for year in range(2019, 2024):
        logger.info("Iniciando descarga del ano %s", year)
        try:
            json_raw = ejecutar_peticion_oa( id_tarea=year, api=API_KEY)
            # Se guarda la respuesta completa, se filtra en trasnformacion
            # Si se necesitan otros metadatos, no hay q volver a descargar
            # Estan aqui
            ruta = guardar_crudo(json_raw, year)
            n_paises = len(json_raw.get("group_by", []))
            logger.info("Ano %s guardado con exito. Paises: %s", year, n_paises)
        
        except Exception as e:
            logger.critical("El script falló definitivamente para el año %s: %s", year, e)
            # Aquí decidir si hacer un 'break' para parar todo o un 'continue' para saltar de año

if __name__ == "__main__":
    main()