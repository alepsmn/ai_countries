import json
from pathlib import Path
from urllib.parse import urlsplit, parse_qsl

from . import config

class PuertaError(Exception):
    """El fichero no cumple el contrato: no debe entrar en GCS/BQ."""

def valida(ruta: Path) -> None:
    n = 0
    with ruta.open("r", encoding="utf-8") as f:
        for i,linea in enumerate(f, start=1):
            linea = linea.strip() # deja la linea sin espacios ni saltos
            if not linea: # si no 
                continue
            try:
                # texto -> dict
                registro = json.loads(linea)
            except json.JSONDecodeError as e:
                raise PuertaError(f"{ruta}:{i} no es NDJSON") from e

            # rango de anios: OA y WB lo llevan en 'year' de primer nivel (int).
            # .get -> None si falta la clave, que tampoco esta en YEARS -> corta igual.
            year = registro.get("year")
            if year not in config.YEARS:
                raise PuertaError(f"{ruta}:{i} anio fuera de rango: {year!r}")

            # request_url no puede llevar credenciales, aunque la ingesta ya las
            # sanee: la puerta es la garantia independiente antes de subir a cloud.
            query = urlsplit(registro.get("request_url", "")).query
            claves = {clave.lower() for clave, _ in parse_qsl(query, keep_blank_values=True)}
            coladas = claves & config.SENSITIVE_PARAMS
            if coladas:
                raise PuertaError(f"{ruta}:{i} credencial en request_url: {sorted(coladas)}")

            n += 1
    if n == 0:
        raise PuertaError(f"{ruta} vacio")