# Fase 1 — Entorno, autenticación y landing crudo

Estado: **cerrada**.
Fechas: abierta 2026-07-09, cerrada 2026-07-13.

Cubre la base sobre la que se apoya todo lo demás: entorno reproducible, credenciales
sin secretos en disco, y un formato de landing que BigQuery pueda cargar sin
transformación previa.

Entregable: **10 ficheros NDJSON válidos** en `data/raw/` (OpenAlex ×5 años, World Bank
×5 indicadores), con trazabilidad de la petición que los generó y sin credenciales
dentro. `mypy` limpio sobre `ingestion/`.

---

## 1. Las piezas y cómo encajan

El pipeline va de una API pública a una tabla consultable. Entre medias hay tres
fronteras que conviene no confundir, porque cada una existe por una razón distinta.

### Proyecto GCP

Es la unidad de **facturación, cuota y permisos (IAM)**. No es una carpeta: es el
límite dentro del cual una identidad tiene o no tiene derechos. Dos proyectos
distintos no comparten permisos por defecto, aunque sean del mismo dueño.

Aquí: `ai-countries-501514`. Todo lo de este pipeline vive dentro.

### Bucket de Cloud Storage (GCS)

Almacenamiento de **objetos**: archivos opacos, sin esquema, inmutables en la
práctica. GCS no sabe qué hay dentro de un `.json`; solo lo guarda y lo sirve.

Es donde aterriza el **raw**: la respuesta de la API tal cual llegó, sin tocar. Su
valor es que si mañana descubres que la transformación tenía un bug, no vuelves a
pegarle a la API — reprocesas desde el bucket. El raw es tu red de seguridad y tu
auditoría: "esto es literalmente lo que la fuente dijo, este día".

Aquí: `gs://ai_countries_raw`, multi-región `US`.

### Dataset y tabla de BigQuery

BigQuery es el **warehouse**: datos con esquema, columnas tipadas, consultables con
SQL. Un *dataset* es el contenedor lógico de tablas (equivalente a un schema en
Postgres) y —esto importa— **tiene una location fija que no se puede cambiar**.

Aquí: dataset `raw`, location `US`.

### Cómo se relacionan: el salto GCS → BigQuery

```
API pública  →  disco local  →  GCS (bucket)  →  BigQuery (dataset.tabla)
   JSON          data/raw/        objeto crudo       filas tipadas
                 landing          inmutable          consultables
```

BigQuery **carga desde GCS** mediante un *load job*: le dices "coge `gs://.../x.json`,
interprétalo como NDJSON, y mételo en `raw.wb_population`". Ese job es gratuito
(no escanea bytes facturables) y es la vía estándar de ingesta batch.

Se podría cargar directo desde local con `load_table_from_file()` y saltarse el
bucket. No lo hacemos: sin el bucket pierdes el raw inmutable, y con él la capacidad
de reprocesar sin re-descargar. El bucket es el punto de rearranque del pipeline.

### Co-locación (la trampa)

Un load job exige que **bucket y dataset compartan location**. No es una
recomendación, es un error duro. Y como la location de un dataset es inmutable,
equivocarse aquí obliga a recrearlo.

Bucket en `US` (multi-región) + dataset en `US` → co-locados. Verificado. Si el
bucket hubiera quedado en `us-central1` (región) habría que haber comprobado la
compatibilidad antes de crear nada.

---

## 2. Decisiones y por qué

### Entorno: `uv`

Sustituye a `requirements.txt` + venv manual. `pyproject.toml` declara las
dependencias directas; `uv.lock` fija el árbol completo y resuelto, que es lo que
hace el entorno **reproducible** (un `requirements.txt` sin pinear no lo es).

`uv sync` reconstruye el entorno exacto. El venv corre Python 3.13.14 contra un
`requires-python = ">=3.12"`.

### Autenticación: ADC, no claves de service account

Intento inicial: descargar una clave JSON de una SA. Bloqueado por política de
organización:

```
iam.disableServiceAccountKeyCreation
```

Esa política existe porque **una clave de SA es un secreto de larga duración**: un
archivo que, si se filtra, da acceso hasta que alguien lo revoque a mano. Es la causa
más común de incidentes en GCP.

La alternativa es **ADC (Application Default Credentials)**: te autenticas con tu
identidad de usuario vía OAuth, y las librerías de Google resuelven las credenciales
solas. No hay fichero de secreto que versionar ni que filtrar.

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project ai-countries-501514
```

El segundo comando es fácil de olvidar y su ausencia produce errores de `serviceusage`
que no dicen lo que pasa. El *quota project* es el proyecto al que se le facturan las
llamadas a la API.

Consecuencia en código: `config.py` ya **no** define `KEY_FILE`, y los clientes se
construyen con `bigquery.Client(project=...)` a secas.

> Nota de aprendizaje: una clave de SA no está "limitada a un proyecto". Identifica a
> una SA, que *vive* en un proyecto, pero puede operar en otros si le conceden roles
> IAM allí. Lo que no cruza fronteras automáticamente son los permisos, no la clave.

### Formato de landing: NDJSON

BigQuery, al cargar JSON, **solo acepta NDJSON** (*newline-delimited JSON*): un objeto
JSON completo por línea, sin indentar, sin comas, sin array envolvente.

```jsonl
{"countryiso3code":"ESP","date":"2024","value":48797875}
{"countryiso3code":"FRA","date":"2024","value":68374591}
```

Razón: es *splittable*. El cargador puede trocear el archivo por saltos de línea y
paralelizar sin parsear el conjunto. Un array JSON de 24k objetos hay que leerlo
entero en memoria antes de saber dónde acaba el primer registro.

Coincide con el `jsonl` que el briefing ya fijaba como formato de landing.

---

## 3. Bug encontrado y corregido

`ingestion/wb_landing.py` escribía cada registro con `json.dump(record, f, indent=2)`
dentro de un bucle, **sin separador**. El resultado no era ni un array JSON ni NDJSON:

```
  "decimal": 0
}{
  "indicator": {
```

1589 fronteras `}{` solo en `datos-POPULATION`. `json.load()` falla con
`Extra data: line 16 column 2`. BigQuery tampoco lo carga.

Fix (`wb_landing.py`), un objeto por línea, sin indentar:

```python
with output_path.open("w", encoding="utf-8") as f:
    for record in data_indic:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
```

**Se corrigió el writer, no el cargador.** Reparar el JSON roto al leerlo habría dejado
el bug vivo en disco para siempre, y el raw habría dejado de ser fiel a la fuente. La
API del Banco Mundial es pública y barata de re-pegar: se regenera y ya.

---

## 4. Trazabilidad: el crudo no es solo el payload

La respuesta de una API no se explica sola. Un fichero con 200 países dentro no dice
**qué consulta lo produjo**, ni cuándo, ni contra qué fuente. Y esa información no se
puede reconstruir después: el filtro (`topics.subfield.id:1702`) vive en el código, y
el código cambia.

Por eso `oa_landing.py` no persiste la respuesta desnuda, sino envuelta:

```python
{
  "source": "openalex",
  "request_url": "https://api.openalex.org/works?filter=...&group_by=...",
  "year": 2023,
  "ingested_at": "2026-07-13T16:43:54.465772+00:00",
  "payload": { ... respuesta literal de OpenAlex ... }
}
```

Cada campo responde a una pregunta que te harás en tres meses:

| Campo | Pregunta que contesta |
|---|---|
| `source` | ¿de dónde salió esto? (habrá varias fuentes en `raw`) |
| `request_url` | ¿qué pregunté exactamente? — el filtro, no la intención del filtro |
| `year` | ¿a qué partición corresponde? (la clave del load job y del MERGE) |
| `ingested_at` | ¿de cuándo es esta foto? — permite detectar crudo rancio |
| `payload` | la respuesta **sin tocar** |

**El payload va anidado, no aplanado.** Es la decisión que importa aquí: mantiene la
frontera entre *lo que dijo la fuente* y *lo que yo anoté sobre la ingesta*. En staging,
`payload.group_by` es de OpenAlex y todo lo demás es mío. Si se aplanaran en el mismo
nivel, un cambio de esquema en OpenAlex podría colisionar con mis metadatos y ya no
sabrías de quién es cada campo.

---

## 5. Incidente: la API key acabó dentro del crudo

Al añadir `request_url` al envoltorio, el secreto entró con ella.

`requests` construye `r.url` a partir de `params`, y la key estaba en `params`. El
resultado se persistió tal cual en los cinco ficheros:

```
"request_url": "https://api.openalex.org/works?filter=...&api_key=<REDACTADA>"
```

**Alcance real:** `data/` está en `.gitignore` y se verificó contra el historial
(`git log --all -- data/`): nunca se commiteó. El secreto no salió de la máquina. Key
rotada, `.env` actualizado, re-ingesta ejecutada.

> **La lección, que es la parte que vale:** un metadato de trazabilidad es una
> **superficie de fuga**. En el momento en que persistes algo que la librería construyó
> por ti (`r.url`, unas cabeceras, el `repr` de un objeto de conexión), estás guardando
> lo que la *librería* decidió meter ahí, no lo que tú creías estar guardando.

### Las dos defensas, y por qué hacen falta las dos

**1. Autenticación por cabecera** — el arreglo de raíz.

```python
headers["Authorization"] = f"Bearer {api}"   # antes: params["api_key"] = api
```

El secreto deja de formar parte de la URL, así que `r.url` nace limpia. OpenAlex acepta
ambas formas; la cabecera es la correcta porque una URL se escribe en logs, en cachés,
en el `Referer` y —como aquí— en ficheros.

**2. `sanear_url()`** — defensa en profundidad.

Filtra de la query cualquier parámetro sensible antes de persistirla. Con la defensa 1
puesta, hoy no elimina nada. Sigue valiendo por dos razones concretas:

- **Redirects.** `r.url` es la URL *final* tras seguir redirecciones, no la que tú
  construiste. No controlas sus parámetros.
- **Regresión.** Si alguien vuelve a meter la key en `params`, el invariante aguanta.
  Convierte *"no hay secreto en el crudo"* de suerte en garantía verificable.

La descomposición es `urlsplit` → `parse_qsl` → filtrar → `urlencode` → `urlunsplit`.
Los porqués de cada elección (`urlsplit` y no `urlparse`, `parse_qsl` y no `parse_qs`,
`keep_blank_values=True`) están comentados en `ingestion/oa_landing.py:114-129`.

Se sanea **en el `return` de `ejecutar_peticion_oa`**, no en `main()`. Así el invariante
*"esta función nunca devuelve una URL con credenciales"* es local y no depende de que
todos los llamadores se acuerden.

---

## 6. Decisiones de consulta a OpenAlex (dos bloqueantes del briefing, resueltos)

Ambas estaban **implícitas en el código**. Declararlas es lo que las convierte en
decisión en vez de en accidente.

### `topics`, no `concepts` (bloqueante 2)

```python
"filter": f"topics.subfield.id:1702,publication_year:{id_tarea}"
```

`concepts.id:C154945302` está **deprecado**: responde, pero la taxonomía está congelada.
OpenAlex migró a `topics`. Se usa el subfield `1702` (*Artificial Intelligence*).

*Consecuencia asumida:* la serie no es comparable con la línea base anterior, que usaba
`concepts`. Por eso se reingestan los cinco años enteros con `topics` — internamente
consistente, aunque rompa con el histórico previo. Preferible a arrastrar una taxonomía
muerta.

### Full counting, no fractional (bloqueante 1)

```python
"group_by": "authorships.countries"
```

Un paper firmado por EEUU y China suma **+1 a cada uno**. Por tanto la suma de países
es mayor que el número de papers, y la colaboración internacional queda inflada.

*Alternativa descartada:* fractional counting (1/n por país) **no es expresable con
`group_by`**. Exigiría paginar los `works` uno a uno y repartir el crédito en cliente —
de una petición por año a decenas de miles.

*Decisión:* **full counting, por coste de ingesta.** Se declara como limitación conocida
y debe repetirse al interpretar el score: mide *participación* en producción de IA, no
*autoría fraccionada*.

---

## 7. Reproducir la fase

```bash
uv sync                                   # entorno exacto desde uv.lock
gcloud auth application-default login     # credenciales, sin claves
gcloud auth application-default set-quota-project ai-countries-501514

.venv/bin/python -m ingestion.wb_landing  # World Bank -> data/raw/worldbank/
.venv/bin/python -m ingestion.oa_landing  # OpenAlex   -> data/raw/openalex/
```

Se ejecutan con `-m` (como módulo, no como script) porque usan imports relativos
(`from . import config`). `python ingestion/oa_landing.py` falla con
`ImportError: attempted relative import with no known parent package`.

Verificación (esto es exactamente lo que BigQuery exigirá):

```bash
.venv/bin/python -c "
import json, pathlib
for f in sorted(pathlib.Path('data/raw/worldbank').glob('*.json')):
    n = sum(1 for l in open(f) if json.loads(l))
    print(f'{f.name}: {n} registros, NDJSON valido')
"
```

---

## 8. Estado verificado

### Infraestructura (2026-07-09)

| Comprobación | Resultado |
|---|---|
| Identidad ADC | usuario (OAuth), sin SA |
| BigQuery: crear load jobs en `ai-countries-501514` | OK (dry-run aceptado) |
| Datasets existentes | ninguno |
| Bucket | `ai_countries_raw`, location `US` |
| Co-locación bucket ↔ dataset | compatible |

### Landing (2026-07-13)

| Comprobación | Resultado |
|---|---|
| `data/raw/openalex/` | 5 ficheros (2019–2023), 1 línea NDJSON c/u, 200 países |
| `data/raw/worldbank/` | 5 ficheros (1 por indicador), 1.325–1.590 líneas NDJSON |
| NDJSON parseable línea a línea | 10/10 OK |
| `api_key` en `request_url` | 0/5 ficheros |
| `mypy ingestion/` | Success: no issues found in 4 source files |
| `data/` en el historial de git | ausente (nunca commiteado) |

Comando de verificación:

```bash
.venv/bin/python -c "
import json, pathlib
for f in sorted(pathlib.Path('data/raw').rglob('*.json')):
    n = sum(1 for l in open(f, encoding='utf-8') if l.strip() and json.loads(l))
    print(f'{f}: {n} lineas NDJSON validas')
"
```

---

## 9. Pendientes

Los seis pendientes que arrastraba esta fase se han resuelto o reclasificado:

| # | Pendiente original | Estado |
|---|---|---|
| 1 | Re-ingesta de World Bank en NDJSON | **hecho** — 5 ficheros regenerados |
| 2 | `oa_landing.py` no era NDJSON / nunca corrió | **hecho** — §4; una línea por año, la respuesta entera |
| 3 | Dos bloqueantes sin declarar | **hecho** — §6 |
| 4 | `docs/` en `.gitignore` | **hecho** — `docs/` versionado; ver `estado.md` §3 |
| 5 | `keyfile.json` (clave viva de otro proyecto) | fichero borrado; **falta revocar en IAM** → `estado.md` §4 |
| 6 | `ai_patents.py` legacy (Snowflake) | eliminado del repo; se reescribirá al patrón landing en su fase |

Sobre el pendiente 2: se guarda **una línea por año** (la respuesta completa envuelta),
no una fila por país. Explotar `group_by` a filas es una **transformación**, y este
módulo es Extract. La explosión se hace en `staging/` con `UNNEST`, que es donde BigQuery
la hace bien y donde queda versionada en dbt.

Lo que no bloqueaba y se aparca deliberadamente vive en **`docs/estado.md` §4**. Ninguno
impide cerrar la fase; se revisitan cuando haya motivo, no por completismo.

---

## 10. Siguiente fase

Loader `landing → GCS → BigQuery`: subir los NDJSON al bucket y lanzar los load jobs
contra el dataset `raw`. Ahí aparece **la idempotencia de verdad** (error B4 del
briefing: un retry de Airflow no debe duplicar filas) y el esquema de las tablas raw.

Es la parte con más señal de Data Engineering de todo el proyecto, y hasta ahora no se
había tocado.
