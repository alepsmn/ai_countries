@pytest.mark.parametrize("url, esperada", [
    ("https://api.openalex.org/works?api_key=SECRETO&filter=x",
     "https://api.openalex.org/works?filter=x"),
    ("https://api.openalex.org/works?filter=x",
     "https://api.openalex.org/works?filter=x"),
])

def test_sanear_url_elimina_credenciales(url, esperada):
    assert sanear_url(url) == esperada