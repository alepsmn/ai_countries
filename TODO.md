# TODO — deuda aparcada deliberadamente

Cosas que **no bloquean ninguna fase**. Viven aquí para dejar de interrumpir el trabajo
en curso. Se revisitan cuando haya un motivo concreto, no por completismo.

Regla: si aparece algo nuevo mientras se trabaja en otra cosa, **se anota aquí**, no se
mete en la fase abierta.

---

## Higiene / seguridad

- [ ] **Revocar `keyfile.json`** (raíz del repo). Es una clave viva de una service account
      de otro proyecto (`datacenter-impact`). Está en `.gitignore`, pero una clave de SA es
      un secreto de larga duración: mientras exista, alguien puede usarla. Revocar en IAM y
      borrar el fichero.

- [ ] **Sacar `docs/` de `.gitignore`.** Los documentos de fase no se versionan ahora mismo.
      Como son el material del que saldrá el README del portfolio, deberían estar en git.
      Revisar antes que ninguno contenga secretos.

## Robustez de la ingesta

- [ ] **`REQUEST_TIMEOUT = 2` es agresivo** (`ingestion/oa_landing.py:35`). Una consulta con
      `group_by` puede tardar más y dispara reintentos innecesarios. `requests` acepta una
      tupla `(connect, read)` — p. ej. `(3.05, 30)` — que separa "no me coge el teléfono" de
      "está tardando en responder". Son fallos distintos y merecen umbrales distintos.

- [ ] **`break` vs `continue` en `main()`** (`ingestion/oa_landing.py:186-188`). Ahora un año
      que falla se registra y se sigue, pero el proceso **termina con exit code 0**: Airflow
      lo daría por bueno con datos incompletos. Propuesta: contar fallos y salir con código
      distinto de 0 si hubo alguno. La tarea falla, pero los años que sí bajaron quedan en
      disco y el retry no los re-descarga.

- [ ] **`SENSITIVE_PARAMS` dentro de `sanear_url()`** (`ingestion/oa_landing.py:121`). Se
      reconstruye el `frozenset` en cada llamada. Con 5 llamadas es irrelevante; como
      constante de módulo sería más idiomático. Nota de estilo, no bug.

## Cobertura

- [ ] **Tests de `ingestion/`.** `tests/` solo tiene `__init__.py`. Candidatos naturales:
      `sanear_url` (función pura, trivial de parametrizar), la política de reintentos con
      `responses`/`requests-mock`, y el envoltorio de `guardar_crudo`.

- [ ] **`ai_patents.py`** fue eliminado (usaba Snowflake + `write_pandas`, ya no arranca).
      Reescribir al patrón landing cuando toque la fuente de patentes.
