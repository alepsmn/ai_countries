"""
Cliente de extraccion para OpenAlex.

Responsabilidades:
    - Ejecutar peticiones a la API de OpenAlex.
    - Aplicar politica de reintentos ante errores recuperables
    - Persistir la respuesta cruda en formato JSON

Este modulo pertenece la fase Extract del pipeline ETL.
NO realiza transformaciones sobre los datos descargados.
"""

import datetime
import json
import logging
import requests
import time

from random import uniform
from typing import Any
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from . import config

logger = logging.getLogger(__name__)

OUTPUT_DIR = config.OA_DIR
BASE_URL = config.OA_BASE_URL
MAIL_TO = config.USER_MAIL
API_KEY = config.OPENALEX_API_KEY

# Mecanismo de reintentos
MAX_RETRIES = 5
REQUEST_TIMEOUT = 2
BACKOFF_BASE = 2 # El tiempo esperara: 1s, 2s, 4s, ...

# Códigos HTTP considerados recuperables.
# Se reintentan porque normalmente representan
# limitación temporal del servicio o errores del servidor.
RETRYABLE_STATUS = [429, 500, 502, 503, 504]

def ejecutar_peticion_oa(id_tarea: int, api: str | None) -> tuple[dict[str, Any], str]:
    """
    Ejecuta una consulta contrala API de OpenAlex.
    
    La peticion incorpora una politica de reintentos con backoff exponencial
    y jitter, para errores transitorios (429, 5xx). Los errores considerados no 
    recuperables se propagan inmediatamente.

    Args:
        id_tarea:
            Ano de publiacion utilizado como filtro de consulta.

        api:
            API Key de OpenAlex. Si es None, la peticion se ejecuta utlizando
            uicamente User-Agent.
        
    Returns:
        Respuesta JSON ya deserializada.

    Raises:
        HTTPError:
            Cuando OpenAlex devuelve un error HTTP no recuperable.
    
        RuntimeError:
            Cuando se agotan todos los reintentos.
    """

    headers = {
        "User-Agent": f"MiAplicacionIA/1.0 (mailto:{MAIL_TO})"
    }
    params  = {
        "filter": f"topics.subfield.id:1702,publication_year:{id_tarea}",
        "group_by": "authorships.countries",
    }

    if api:
        headers["Authorization"] = f"Bearer {api}"

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(BASE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status() 

            data: dict[str, Any] = r.json()
            return data, sanear_url(r.url)

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
            wait = BACKOFF_BASE ** attempt + uniform(0, 0.5)
            logger.info("Reintentando en %ss...", wait)
            time.sleep(wait)
        
    raise RuntimeError(
        "OpenAlex no respondió tras %s intentos (%s)" %(MAX_RETRIES, id_tarea)
    ) from last_exc # Encadena la excepción original para conservar la causa raíz.

def sanear_url(url: str) -> str:
    """
    Elimina de la query los parametros considerados sensibles.

    La URL se persiste en el fichero crudo para trazabilidad, por lo que no
    puede contener credenciales aunquela peticion se autentique por cabecera.
    """
    partes = urlsplit(url) # urlparse separa el campo params -> daria tupla de 6 no 5
    query = [
        # parse_qs - dict[str, list[str]]; parse_sql - list[tuple[str, str]] lo que urlencode come, ademas de preservar orden y duplicados
        (clave, valor) for clave, valor in parse_qsl(partes.query, keep_blank_values=True)
        if clave.lower() not in config.SENSITIVE_PARAMS
    ]
    return urlunsplit(partes._replace(query=urlencode(query)))

def guardar_crudo(datos: dict[str, Any], year: int) -> Path:
    """
    Persiste la respuesta original de OpenAlex en disco.

    Los datos se almacenan sin modificaciones para mantener una copia
    reproducible de la extracción. Las transformaciones posteriores deben
    realizarse sobre estos ficheros y no sobre la respuesta en memoria.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"openalex_{year}.json"

    with  output_path.open("w", encoding="utf-8") as f:
        # NDJSON -> BigQuery; dumps - devuelve string, dump - escribe a disco
        f.write(json.dumps(datos, ensure_ascii=False) + "\n") # ensure_ascii=False -> tildes/ñ legibles
    return output_path

# ------------------------------------------------------------------------------------------------------------

def main() -> None:
    """
    Orquesta la extracción para todos los años configurados.

    Para cada año:

    1. Descarga los datos desde OpenAlex.
    2. Guarda la respuesta cruda.
    3. Registra el resultado en el log.

    Si un año falla, el error queda registrado para decidir posteriormente
    si continuar o detener el pipeline.
    """
    config.configure_logging()
    if not API_KEY:
        raise RuntimeError(
            "Falta API_KEY de OpenAlex en el entorno (.env) Revisar"
        )

    for year in config.YEARS:
        logger.info("Iniciando descarga del ano %s", year)
        try:
            json_raw, url = ejecutar_peticion_oa( id_tarea=year, api=API_KEY)
            # Se guarda la respuesta completa, se filtra en trasnformacion
            # Si se necesitan otros metadatos, no hay q volver a descargar
            # Estan aqui
            envoltorio = {
                "source": "openalex",
                "request_url": url,
                "year": year,
                "ingested_at": datetime.datetime.now(datetime.UTC).isoformat(),
                "payload": json_raw
            }
            guardar_crudo(envoltorio, year)
            n_paises = len(json_raw.get("group_by", []))
            logger.info("Ano %s guardado con exito. Paises: %s", year, n_paises)
        
        except Exception as e:
            logger.critical("El script falló definitivamente para el año %s: %s", year, e)
            # Aquí decidir si hacer un 'break' para parar todo o un 'continue' para saltar de año

if __name__ == "__main__":
    main()