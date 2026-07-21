"""
Tests de la puerta de validacion tal y como la usa el uploader.

`validacion.valida` ya esta probada fichero a fichero. Lo que se prueba aqui es
`validar_archivos`: el escalon que recorre un directorio y decide si ese
directorio entra en GCS o no entra ninguno de sus ficheros.

Es logica pura -- descubre rutas y llama a la puerta, no toca la API de Google --
asi que no hace falta ningun mock: basta un directorio de mentira en `tmp_path`.
"""

import json
from pathlib import Path

import pytest

from gcs.upload_gcs import validar_archivos
from ingestion import config
from ingestion.validacion import PuertaError


def escribe_ndjson(destino: Path, year: int) -> Path:
    """Escribe un fichero que cumple el contrato salvo por el `year` que se pida."""
    registro = {
        "source": "openalex",
        "request_url": "https://api.openalex.org/works?filter=x",
        "year": year,
        "ingested_at": "2026-07-21T00:00:00Z",
        "payload": {},
    }
    destino.write_text(json.dumps(registro) + "\n", encoding="utf-8")
    return destino


def test_devuelve_los_ficheros_que_pasan(tmp_path: Path) -> None:
    """Camino feliz: si todos cumplen, devuelve exactamente esos ficheros."""
    esperados = {
        escribe_ndjson(tmp_path / f"openalex_{year}.json", year)
        for year in config.YEARS
    }

    assert set(validar_archivos(tmp_path)) == esperados


def test_directorio_vacio_corta(tmp_path: Path) -> None:
    """Un run que no encuentra datos no es un run correcto que sube cero ficheros.

    Sin este corte `main` terminaria con exit code 0 y Airflow pintaria la tarea
    en verde habiendo dejado el bucket sin tocar.
    """
    with pytest.raises(PuertaError):
        validar_archivos(tmp_path)


def test_un_solo_fichero_malo_corta_el_directorio_entero(tmp_path: Path) -> None:
    """La unidad de la decision es el directorio, no el fichero.

    Cuatro ficheros validos y uno con un anio fuera de `config.YEARS`: la funcion
    no devuelve los cuatro buenos, no devuelve nada. Esa es la propiedad que hace
    que `main` pueda validar todo antes de subir nada.
    """
    for year in list(config.YEARS)[:4]:
        escribe_ndjson(tmp_path / f"openalex_{year}.json", year)
    escribe_ndjson(tmp_path / "openalex_futuro.json", config.YEAR_END + 1)

    with pytest.raises(PuertaError):
        validar_archivos(tmp_path)


def test_ignora_lo_que_no_es_json(tmp_path: Path) -> None:
    """El glob es `*.json`: un README o un `.tmp` a medio escribir no es un fallo."""
    escribe_ndjson(tmp_path / "openalex_2019.json", 2019)
    (tmp_path / "notas.txt").write_text("esto no es un landing\n", encoding="utf-8")

    assert validar_archivos(tmp_path) == [tmp_path / "openalex_2019.json"]
