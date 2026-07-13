"""
Tests de la capa de extraccion de OpenAlex.

Cubren `sanear_url`, que es la garantia de que ninguna credencial acaba
escrita en el fichero crudo (la URL se persiste para trazabilidad).
"""

import pytest

from ingestion.oa_landing import sanear_url


@pytest.mark.parametrize(
    "url, esperada",
    [
        # El parametro sensible se elimina y el resto de la query sobrevive.
        (
            "https://api.openalex.org/works?api_key=SECRETO&filter=x",
            "https://api.openalex.org/works?filter=x",
        ),
        # Una URL sin credenciales no se toca.
        (
            "https://api.openalex.org/works?filter=x",
            "https://api.openalex.org/works?filter=x",
        ),
        # La comparacion es insensible a mayusculas.
        (
            "https://api.openalex.org/works?API_KEY=SECRETO&filter=x",
            "https://api.openalex.org/works?filter=x",
        ),
        # Los demas nombres de SENSITIVE_PARAMS tambien caen.
        (
            "https://api.openalex.org/works?token=SECRETO&access_token=OTRO&filter=x",
            "https://api.openalex.org/works?filter=x",
        ),
        # Si la credencial era el unico parametro, la query queda vacia.
        (
            "https://api.openalex.org/works?api_key=SECRETO",
            "https://api.openalex.org/works",
        ),
    ],
)
def test_sanear_url_elimina_credenciales(url: str, esperada: str) -> None:
    assert sanear_url(url) == esperada


def test_sanear_url_preserva_orden_y_duplicados() -> None:
    """`parse_qsl` (no `parse_qs`) conserva orden y claves repetidas."""
    url = "https://api.openalex.org/works?filter=a&api_key=X&filter=b"
    assert sanear_url(url) == "https://api.openalex.org/works?filter=a&filter=b"
