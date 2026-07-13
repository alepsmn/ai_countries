# Fase 2 — Loader: landing → GCS → BigQuery

Estado: **abierta**. Abierta 2026-07-13.

Entregable: los ficheros de `data/raw/` viven en GCS y en dos tablas de BigQuery
(`raw.openalex`, `raw.worldbank`), cargadas por un loader **idempotente**: ejecutarlo dos
veces seguidas deja la base exactamente igual que ejecutarlo una vez.

Esta es la fase con más señal de Data Engineering del proyecto. Hasta ahora el pipeline
bajaba ficheros; a partir de aquí *escribe en un warehouse*, y escribir es donde se rompen
las cosas: un retry de Airflow que duplica filas, un esquema que cambia solo, un dato
viejo que entra sin que nadie lo mire.

---

## 0. Antes de empezar: cerrar fase 1 de verdad — **hecho**

Fase 1 estaba cerrada en el documento pero no en git. Abrir fase 2 encima de eso mezclaba
los diffs y hacía la fase imposible de revisar. Resuelto:

- Los cambios de `oa_landing.py` (reintentos, `sanear_url`, docstrings) están commiteados.
- `tests/oa_test.py` no colecciona**ba**: le faltaban los imports. Reconstruido, tipado y
  ampliado — 6 tests en verde, `mypy --strict` limpio.
- `tablas/` (código muerto de Snowflake) borrado.
- La documentación, centralizada y versionada (ver `estado.md` §3).

El detalle vive en `estado.md`. Este documento ya solo habla de fase 2.

---

## 1. Las tres decisiones que fijan la fase

### 1.1 Esquema de RAW: metadatos tipados + `payload JSON`

```
source       STRING
request_url  STRING
year         INT64
ingested_at  TIMESTAMP
payload      JSON      -- la respuesta de la API, intacta
```

La pregunta de fondo no era "¿qué columnas pongo?" sino **quién es el dueño del esquema**.

Con `autodetect=True` el dueño es *la respuesta de la API*: OpenAlex añade un campo y tu
tabla cambia de forma sola; un campo que venga vacío en todas las filas de un fichero
simplemente no genera columna. RAW deja de tener contrato.

Con un esquema declarado entero a mano el dueño eres tú, pero acoplas RAW a la forma
*actual* de la API: cualquier cambio upstream es un cambio de código y un load job roto.

La tercera opción es la que ya se eligió en fase 1 sin darle este nombre. Cuando se
decidió guardar *una línea por año* en vez de una fila por país, se dijo: **RAW es una
copia fiel; la explosión a filas es una transformación y vive en dbt/staging**. La
traducción a BigQuery de esa misma frase es: metadatos tipados como columnas, respuesta
entera en una columna `JSON`. El esquema de RAW queda estable para siempre y todo lo que
la API decida cambiar cae dentro del `JSON`, donde no rompe nada.

La explosión, en `staging/`, con `UNNEST(JSON_QUERY_ARRAY(payload, '$.group_by'))`. Ahí sí
se tipa y ahí sí hay tests de dbt.

> RAW no está para consultarse: está para poder reconstruir. Un esquema rígido en RAW es
> endurecer la capa equivocada.

### 1.2 Idempotencia: partición por año + sobreescritura de partición

El briefing (error B4) pedía `MERGE`. Ese `MERGE` era contra las tablas EAV viejas, donde
se acumulaban filas país-año-indicador. En un RAW donde **un fichero = una fila** no hay
nada que fusionar: solo hay que **pisar**.

El patrón: tabla con *integer-range partitioning* sobre `year`, y el load job escribiendo
con `WRITE_TRUNCATE` contra el decorador de partición (`raw.openalex$2021`). Reingestar
2021 pisa 2021; los otros cuatro años ni se leen. Encaja de forma natural con un DAG que
tenga **una tarea por año**, que es lo que va a haber en fase 4.

*A verificar contra la documentación de BigQuery, no dar por hecho:* el decorador de
partición está fuera de toda duda para tablas particionadas por fecha/ingesta. Para
integer-range hay que confirmar la sintaxis del decorador y que el load job la acepte. Si
pelea, dilo en el doc y baja a la alternativa — pero pruébalo antes de rendirte, porque el
patrón es el que se cuenta en una entrevista.

*Alternativa perezosa:* `WRITE_TRUNCATE` de la tabla entera. Imposible de romper, honesta
a esta escala (5 filas), y con cero señal DE. No escala a patentes, que es donde el
particionado deja de ser decorativo.

### 1.3 GCS: ruta fija + versionado de objetos

Ruta fija (`openalex/openalex_2021.json`), sobreescrita en cada run. Mantiene el loader
tonto — sabe exactamente qué objeto cargar, sin lógica de "¿cuál es el prefijo más
reciente?" — y concentra la idempotencia en un solo sitio: el load job.

El coste de sobreescribir **no es el disco** (son kilobytes; GCS cobra céntimos). Es el
**replay**. World Bank *revisa* el PIB de años pasados de forma retroactiva, y OpenAlex
sigue indexando papers de 2021 hoy. El fichero de 2021 de julio y el de noviembre **no son
el mismo fichero**. Si el score de un país se mueve entre dos runs, sin replay no puedes
contestar la pregunta que te van a hacer: *¿cambió tu código o cambiaron los datos?*

La salida no es complicar las rutas, es **activar object versioning en el bucket**. GCS
conserva las generaciones anteriores del mismo objeto automáticamente: ruta fija para el
loader, histórico para ti, cero código. Con una *lifecycle rule* de expiración de versiones
no-actuales para que no crezca sin control.

> El histórico va en la versión del objeto, no en el nombre del objeto.

---

## 2. El problema que aparece al mirar los ficheros: los dos landings no son simétricos

Esto no estaba previsto y es el trabajo real de la fase.

**OpenAlex** escribe una línea por año, con envoltorio:

```json
{"source": "openalex", "request_url": "...", "year": 2021,
 "ingested_at": "2026-07-13T16:43:53Z", "payload": {"meta": {...}, "group_by": [...]}}
```

**World Bank** escribe un registro crudo de la API por línea, **sin envoltorio**:

```json
{"indicator": {...}, "country": {...}, "countryiso3code": "AFE",
 "date": "2024", "value": 1252564134751.16, ...}
```

Consecuencias, en orden de gravedad:

1. **El esquema de §1.1 no se puede aplicar a WB.** No hay `payload`, no hay `source`, no
   hay `ingested_at`, no hay `request_url`. WB no tiene trazabilidad: mirando el fichero no
   se puede saber qué petición lo generó ni cuándo.
2. **La granularidad de la tarea es distinta.** La unidad que un retry vuelve a descargar
   es, en OA, *el año* (una petición por año). En WB es *el indicador* (una petición
   paginada por indicador; los años vienen todos dentro). **Esa unidad es la clave de
   idempotencia** — no se puede copiar la de OA a WB sin pensar.
3. **El nombre del fichero de WB lleva la fecha** (`datos-GDP_USD-20260709.json`), lo que
   contradice de plano la ruta fija de §1.3: cada run deja un fichero nuevo y el loader no
   sabe cuál cargar.
4. `wb_landing.py` duplica la escritura a disco en vez de usar `guardar_crudo`. El propio
   código ya lo admite: `# Extraer helper - OA y WB usan la misma estructura`.

Lo que hay que decidir aquí es **qué es una fila de `raw.worldbank`**, y la respuesta debe
salir de la misma regla que ya se aplicó en OA: *la fila de RAW es la unidad que se
descarga*. Si la unidad de WB es el indicador, la fila es el indicador y el `payload` es la
lista de registros que devolvió la API. Si se decide que la fila es el registro, entonces
el envoltorio va repetido en cada línea y hay que asumir qué significa eso.

Ninguna de las dos es gratis. Lo que no vale es dejarlo como está.

---

## 3. El dato viejo: World Bank trae 2024 y OpenAlex no

`config.py` declara `YEAR_END = 2023` (razonado: 2024 sigue indexándose en OA y compararlo
sesga a la baja). Pero los ficheros de WB en disco, bajados el 09-07, contienen **2019–2024**:

```
datos-GDP_USD-20260709.json                 ['2019'..'2024']  n=1590
datos-GDP_PER_CAPITA_USD-20260709.json      ['2019'..'2024']  n=1590
datos-RD_GDP_PCT-20260709.json              ['2019'..'2024']  n=1590
datos-RESEARCHERS_PER_MILLION-20260709.json ['2019'..'2024']  n=1590
datos-POPULATION-20260709.json              ['2019'..'2023']  n=1325   <- ni siquiera coincide con sus hermanos
```

Es el **error A3 del briefing** (rango temporal inconsistente), vivo, en el landing. Se
bajaron con un `YEAR_END` anterior y `config` cambió después. Reingestar WB hoy lo arregla.

Pero arreglar el síntoma no es la lección. **La lección es que nadie se dio cuenta**, y no
se dio cuenta porque *nada verifica que el fichero en disco cumpla el contrato que declara
`config.py`*. El landing y la configuración pueden divergir en silencio, y el loader —
alegremente— habría metido esa divergencia en BigQuery, de donde sale el score.

De ahí sale un requisito que esta fase no tenía y ahora sí:

> **Puerta de validación antes de cargar.** Un fichero no entra en GCS/BQ sin que se
> compruebe que cumple su contrato: NDJSON válido, rango temporal dentro de `config.YEARS`,
> sin credenciales en `request_url`, no vacío. Si falla, la tarea falla — no se carga "lo
> que haya".

Esto es exactamente lo que distingue un pipeline de un script: **el script asume que su
input es correcto; el pipeline lo comprueba y falla ruidosamente cuando no lo es.**

---

## 4. Orden de build

Por dependencia, no por preferencia.

1. ~~**Cerrar fase 1**: commit del trabajo pendiente + arreglar `tests/oa_test.py`.~~ **hecho** (§0).
2. **Simetrizar los landings** (§2): decidir la fila de `raw.worldbank`, poner envoltorio y
   trazabilidad en WB, quitar la fecha del nombre del fichero, extraer el helper de
   escritura compartido.
3. **Puerta de validación** (§3) + reingesta de WB con el rango correcto.
4. **Infra GCP**: bucket con versioning + lifecycle; dataset `raw` en la misma *location*
   que el bucket (`US`, ya declarado en `config.py` — si no coinciden, el load job falla).
5. **Uploader a GCS**: ruta fija.
6. **Load jobs a BigQuery**: esquema explícito de §1.1, particionado, `WRITE_TRUNCATE` por
   partición.
7. **Tests** (`pytest`): la puerta de validación es una función pura, se parametriza sola.
   El loader se testea con mocks del cliente de BQ — no se llama a BigQuery en un test.

---

## 5. Cómo se demuestra que la fase está cerrada

No basta con "se cargó". La afirmación que hay que poder defender es **idempotencia**:

- Correr el loader dos veces seguidas y comprobar que `COUNT(*)` no cambia. Ese es *el*
  experimento de la fase; si duplica, no está hecho.
- Reingestar **un solo año** y comprobar que las demás particiones no se tocaron
  (`_PARTITIONTIME` / bytes procesados / `last_modified_time` de la partición).
- `raw.openalex` con 5 filas (una por año) y `raw.worldbank` coherente con la fila que se
  haya decidido en §2.
- Ningún año fuera de `config.YEARS` en ninguna de las dos tablas. Consulta explícita, no
  confianza.
- `mypy` limpio, `pytest` en verde.

---

## 6. Pendientes / decisiones abiertas

- [ ] **La fila de `raw.worldbank`** (§2). Bloquea el esquema y el mecanismo de
      idempotencia de esa tabla.
- [ ] **Decorador de partición sobre integer-range** (§1.2). Verificar contra la doc de BQ
      antes de dar por bueno el patrón.
- [ ] **Idempotencia de WB**: si su unidad es el indicador y no el año, su clave de
      sobreescritura no puede ser una partición por `year`. Decidir junto con §2.
- [ ] `REQUEST_TIMEOUT = 2` en OA sigue siendo agresivo (ver `estado.md` §4). Si el loader
      va a correr en Airflow, esto pasa de nota de estilo a fallo intermitente.
