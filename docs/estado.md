# Estado — el marcador único

**Un solo sitio para saber qué queda.** Si un hallazgo no está en este documento, es nuevo
de verdad; si está, es deuda conocida y no una regresión.

Tiene tres secciones y la diferencia entre ellas importa:

1. **Los 11 errores del briefing** — la deuda con la que arrancó el proyecto. Cada uno se
   cierra en la fase que construye la capa donde vive.
2. **Higiene** — trabajo barato que quita niebla. No bloquea nada, pero mientras esté
   pendiente hace que el repo mienta sobre sí mismo.
3. **Deuda aparcada** — lo que apareció trabajando en otra cosa. **Regla: si aparece algo
   nuevo mientras trabajas en una fase, se anota aquí, no se mete en la fase abierta.**

Actualizado: 2026-07-15.

---

## 1. Los 3 bloqueantes — cerrados

| # | Decisión | Estado |
|---|---|---|
| 1 | Coautoría OpenAlex: **full counting** (fractional no es expresable con `group_by`) | cerrado — `fase-01` §6 |
| 2 | Taxonomía: **`topics.subfield.id:1702`**, no `concepts` (deprecado) | cerrado — `fase-01` §6 |
| 3 | Korea: causa raíz = **split de nombres del modelo EAV** (`Korea, Rep.` vs `Korea, Republic of`), no un hueco de World Bank | diagnosticado — el arreglo se implementa en dbt (ver B5/B7) |

## 2. Los 11 errores

**Van 5 cerrados, 6 abiertos.**

### A — Corrupción de datos

| # | Error | Estado | Dónde se cierra |
|---|---|---|---|
| A1 | `country_code` en patentes = jurisdicción de la oficina, no del inventor | **abierto** | fase patentes — *el módulo no existe* |
| A2 | Doble conteo por `UNNEST(cpc)`; falta `COUNT(DISTINCT publication_number)` | **abierto** | fase patentes — *el módulo no existe* |
| A3 | Rango temporal inconsistente entre fuentes | **cerrado** | código y datos alineados: WB reingestado con `YEAR_END=2023` (los 5 ficheros, 2019–2023), y la **puerta de validación** (`ingestion/validacion.py`) corta cualquier año fuera de `config.YEARS` antes de subir. Ya no puede divergir en silencio |

### B — Arquitectura

| # | Error | Estado | Dónde se cierra |
|---|---|---|---|
| B4 | Sin idempotencia: la ingesta hace `append`; un retry de Airflow duplica | **abierto** | **fase 2** — es el objeto de la fase |
| B5 | Modelo EAV: `COUNTRY_NAME` repetido en cada RAW → causa el bug de Korea | **abierto** | dbt `staging/` — *`dbt/models/` está vacío* |
| B6 | `VW_AI_KPI` definida 3 veces con cuerpos distintos | **abierto** | dbt `marts/` — *`dbt/models/` está vacío* |
| B7 | Parche manual de Korea (5 valores hardcoded, sin fuente ni fecha) | **abierto** (causa ya diagnosticada, ver bloqueante 3) | dbt — desaparece solo al arreglar B5 |

### C — Robustez

| # | Error | Estado | Verificado |
|---|---|---|---|
| C8 | `requests.get` sin `timeout=` | **cerrado** | `oa_landing.py:84`, `wb_landing.py:49` |
| C9 | OpenAlex: `r.json()` sin comprobar `status_code` | **cerrado** | `raise_for_status()` + política de reintentos |
| C10 | Rutas absolutas `C:\Users\alex\...` | **cerrado** | env vars + `Path` en `config.py`. El código muerto de `tablas/` que las contenía se borró |
| C11 | `except:` desnudo en `iso2_to_country` | **cerrado** | la función ya no existe; `grep "except:"` no da nada |

### Por qué parece que los errores "vuelven" y no vuelven

Los 6 abiertos **no están en el código que ya escribiste**. Están en capas que todavía no
existen:

- **B4** → el loader a BigQuery. Es la fase 2.
- **B5, B6, B7** → dbt. `dbt/models/staging|intermediate|marts` están *vacíos*.
- **A1, A2** → patentes. El módulo se borró y se reescribirá en su fase.

Por eso cada vez que te acercas a una capa nueva "aparece" un error. No aparece: estaba
declarado desde el día uno, esperando en la capa que ibas a construir. Es secuenciación
correcta, no trabajo repetido. Lo que faltaba era este marcador.

**A3 ya está cerrado** (antes parecía una regresión: el código estaba bien y lo viejo era
el fichero). Se cerró reingestando WB y, sobre todo, construyendo la **puerta de validación**
que impide que *artefacto* y *código* vuelvan a divergir en silencio. Esa distinción entre
*código correcto* y *artefacto correcto* fue, en sí misma, media fase 2.

---

## 3. Higiene

- [x] **Borrar `tablas/`** (código Snowflake muerto: el hardcode de Korea, la `VW_AI_KPI`
      triplicada, las rutas `C:\Users\alex\...`). Mientras siguiera en el árbol, un `grep`
      encontraba errores **ya cerrados** y parecía que nada avanzaba. Está en el historial
      de git: no se pierde nada. — hecho en `8b1e88f`
- [x] **Commitear el trabajo de fase 1** — hecho en `c770e6e` + `8b1e88f`
- [x] **`tests/oa_test.py` no colecciona.** Le faltaban los imports. `8b1e88f` lo borró
      entero en vez de arreglarlo; se ha reconstruido con los imports, tipado para `mypy
      --strict` y ampliado (mayúsculas, el resto de `SENSITIVE_PARAMS`, query que queda
      vacía, orden y duplicados). — 6 tests en verde
- [x] **Sacar `docs/` de `.gitignore`.** Los documentos de fase no se versionaban: el repo
      que ve un lector externo no documentaba nada. Revisados por secretos antes de
      versionar (la API key ya rotada que aparecía en `fase-01` §5 quedó redactada). — hecho
- [x] **Centralizar la documentación.** `TODO.md` (raíz) absorbido por este documento;
      `docs/CLAUDE.md` renombrado a `docs/briefing.md`; `README.md` escrito como portada.

---

## 4. Deuda aparcada

Cosas que **no bloquean ninguna fase**. Viven aquí para dejar de interrumpir el trabajo en
curso. Se revisitan cuando haya un motivo concreto, no por completismo.

### Seguridad

- [ ] **Revocar la clave de service account en IAM.** El fichero `keyfile.json` ya no está
      en el disco ni en git, pero **borrar el fichero no revoca la clave**: sigue viva en el
      proyecto `datacenter-impact` hasta que se elimine en IAM. Una clave de SA es un
      secreto de larga duración; mientras exista, quien tenga una copia puede usarla.

### Robustez de la ingesta

- [ ] **`REQUEST_TIMEOUT = 2` es agresivo** (`ingestion/oa_landing.py:35`). Una consulta con
      `group_by` puede tardar más y dispara reintentos innecesarios. `requests` acepta una
      tupla `(connect, read)` — p. ej. `(3.05, 30)` — que separa "no me coge el teléfono" de
      "está tardando en responder". Son fallos distintos y merecen umbrales distintos.
      **Sube a bloqueante en fase 4**: bajo Airflow deja de ser nota de estilo y se convierte
      en fallo intermitente.

- [ ] **`break` vs `continue` en `main()`** (`ingestion/oa_landing.py:186-188`). Ahora un año
      que falla se registra y se sigue, pero el proceso **termina con exit code 0**: Airflow
      lo daría por bueno con datos incompletos. Propuesta: contar fallos y salir con código
      distinto de 0 si hubo alguno. La tarea falla, pero los años que sí bajaron quedan en
      disco y el retry no los re-descarga.

- [x] **`SENSITIVE_PARAMS` como constante de módulo** — hecho. Subido a `config.py` como
      constante compartida; lo usan `sanear_url` (ingesta) y la puerta de validación
      (`validacion.py`). Dejó de reconstruirse por llamada y hay una sola fuente.

### Cobertura

- [ ] **Tests de la política de reintentos.** `sanear_url` ya está cubierto. Faltan los dos
      candidatos naturales que quedan: los reintentos con backoff (con `responses` o
      `requests-mock`, sin pegarle a la API) y el envoltorio de `guardar_crudo`.

- [ ] **`ai_patents.py`** fue eliminado (usaba Snowflake + `write_pandas`, ya no arranca).
      Reescribir al patrón landing cuando toque la fuente de patentes. Arrastra A1 y A2.
