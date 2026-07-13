# AI Countries

Pipeline ELT que clasifica países y bloques geopolíticos (NATO / BRICS / EU / OTHER) por
capacidad en Inteligencia Artificial, a partir de tres fuentes públicas.

**Lo que se construye aquí es el pipeline, no el modelo.** El score es determinista,
ponderado y auditable — sin clustering ni ML. La señal de ingeniería está en cómo se
ingiere, se valida, se transforma y se orquesta el dato: idempotencia, tipado estricto,
tests y trazabilidad de la fuente.

> **Estado: en construcción.** El landing crudo funciona (fase 1, cerrada). El loader a
> BigQuery está en curso (fase 2). dbt y Airflow todavía no existen — ver
> [docs/estado.md](docs/estado.md) para el marcador honesto de qué está hecho y qué no.

---

## Arquitectura

```
API pública  →  disco local  →  GCS (bucket)  →  BigQuery  →  dbt  →  score
   JSON          data/raw/       objeto crudo      raw.*       staging/
                 landing         inmutable         tipado      marts/
                    ↑                ↑                ↑           ↑
                 fase 1           fase 2          fase 2      fase 3
```

Todo orquestado por Airflow invocando dbt-core (fase 4).

| Capa | Herramienta | Por qué |
|---|---|---|
| Ingesta | Python 3.13, type hints, `mypy --strict`, `pytest` | contratos verificables, no scripts |
| Landing | GCS, NDJSON | raw inmutable = punto de rearranque sin re-pegar a la API |
| Warehouse | BigQuery | las patentes ya viven ahí (`patents-public-data`) |
| Transformación | dbt-core | una sola definición de cada modelo, versionada y testeada |
| Orquestación | Airflow | retries idempotentes, una tarea por año |
| Entorno | `uv` + `uv.lock` | reproducible de verdad |

## Fuentes

| Fuente | Qué aporta | Nota |
|---|---|---|
| **OpenAlex** (API) | artículos de IA por país y año, con citas | `topics.subfield.id:1702`. **Full counting**: un paper US–China suma +1 a cada uno |
| **World Bank** (API) | población, PIB, PIB per cápita, % I+D sobre PIB, investigadores por millón | paginada, con filtro de agregados |
| **Google Patents** (BigQuery) | patentes de IA vía CPC `G06N%` | pendiente — se reescribe en su fase |

## El score

Cuatro dimensiones ortogonales, peso `0.25` cada una como baseline documentado:

1. **Producción** — volumen (nº de artículos) y calidad (citas normalizadas por año). No se funden.
2. **Traslación** — patentes de IA y ratio patentes/artículos (eficiencia traslacional).
3. **Capital humano** — investigadores per cápita. *Proxy grueso: STEM ≠ IA.*
4. **Corrección de tamaño** — regla transversal: cada métrica en forma absoluta y relativa.

**Regla de coherencia:** el PIB y el `% I+D sobre PIB` son **inputs, no componentes**. Entran
como eje de normalización o de eficiencia (output/input), **nunca sumados al score**.

---

## Cómo se ejecuta

```bash
uv sync                                             # entorno exacto desde uv.lock
gcloud auth application-default login               # ADC, sin claves de service account
gcloud auth application-default set-quota-project ai-countries-501514

cp .env.example .env                                # rellenar OPENALEX_API_KEY, USER_MAIL

uv run python -m ingestion.wb_landing               # World Bank -> data/raw/worldbank/
uv run python -m ingestion.oa_landing               # OpenAlex   -> data/raw/openalex/
```

Se invocan con `-m` (como módulo, no como script) porque usan imports relativos.

```bash
uv run pytest      # tests
uv run mypy        # tipado estricto sobre ingestion/ y tests/
```

La autenticación va por **ADC**, no por fichero de clave: la organización prohíbe crear
claves de service account (`iam.disableServiceAccountKeyCreation`), y con razón — una clave
de SA es un secreto de larga duración. No hay ningún fichero de credenciales que versionar.

## Estructura

```
ingestion/          # Python tipado y testeado — capa Extract
  config.py         #   rango temporal único + rutas por env var (fuente de verdad)
  oa_landing.py     #   OpenAlex: reintentos con backoff, saneado de URL
  wb_landing.py     #   World Bank: paginación
tests/              # pytest sobre ingestion/
data/raw/           # landing crudo (NDJSON, fuera de git)
docs/               # ver abajo
```

## Documentación

| Documento | Qué contiene |
|---|---|
| **[docs/estado.md](docs/estado.md)** | **El marcador único**: los 11 errores del briefing (qué está cerrado y qué no), la higiene y la deuda aparcada. Empieza aquí. |
| [docs/briefing.md](docs/briefing.md) | El briefing original: objetivo, stack decidido, los 11 errores y el orden de build. Contexto inmutable. |
| [docs/fase-01-entorno-auth-landing.md](docs/fase-01-entorno-auth-landing.md) | Entorno, autenticación y landing crudo. **Cerrada.** |
| [docs/fase-02-loader-gcs-bigquery.md](docs/fase-02-loader-gcs-bigquery.md) | Loader idempotente a GCS y BigQuery. **Abierta.** |

Cada fase se documenta *mientras* se construye, con las decisiones y su porqué — incluidos
los errores cometidos y lo que enseñaron. De ahí sale este README.
