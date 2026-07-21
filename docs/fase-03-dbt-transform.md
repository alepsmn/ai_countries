# Fase 3 — dbt: de RAW a un score auditable

Estado: **abierta** (2026-07-21).

Entregable: `raw.openalex` y `raw.worldbank` —dos tablas que hoy son copias fieles de lo
que devolvió una API— convertidas en un modelo consultable, tipado y probado, con el score
compuesto en `marts/` y una sola definición de cada cosa.

Es la fase donde mueren tres de los cinco errores que quedan abiertos: **B5** (modelo EAV),
**B6** (`VW_AI_KPI` definida tres veces) y **B7** (el parche manual de Korea). No es
casualidad que caigan juntos: son el mismo error visto desde tres sitios.

---

## 0. De dónde se parte

Lo que hay en el repo, para no descubrirlo a mitad:

- `dbt/models/`, `dbt/seeds/` y `dbt/tests/` **existen y están vacíos**. No hay
  `dbt_project.yml`.
- **dbt no está instalado.** No está en `pyproject.toml` ni en el `.venv`.
- `~/.dbt/profiles.yml` existe pero es de **otro proyecto** (`datacenter_impact`) y se
  autentica con `keyfile`. Aquí no vale: la organización prohíbe crear claves de service
  account y este proyecto va por ADC (`config.py` lo documenta). El perfil de
  `ai_countries` tendrá que usar `method: oauth`, y la `location` debe ser `US` — la misma
  del bucket y del dataset, o los jobs fallan.

Lo que sí está sólido y esta fase da por bueno: RAW cargado y verificado (5 filas en
`openalex`, 6625 en `worldbank`), cadena idempotente de disco a BigQuery, y la puerta de
validación enchufada y probada.

---

## 1. La prueba documental de Korea

Antes de decidir nada, los datos reales. Esto es lo que hay hoy en los ficheros crudos:

| fuente | identificador | nombre |
|---|---|---|
| OpenAlex | `https://openalex.org/countries/KR` | `Korea, Republic of` |
| World Bank | `id: KR`, `countryiso3code: KOR` | `Korea, Rep.` |

Un país. **Dos nombres distintos y un código idéntico.**

Ahí está el bug entero, sin necesidad de teoría. El pipeline viejo juntaba las fuentes por
`COUNTRY_NAME` porque el modelo EAV repetía el nombre en cada tabla RAW; `Korea, Republic
of` no es igual a `Korea, Rep.`, el join no casaba, y los investigadores de Corea del Sur
"desaparecían". La reacción fue escribir cinco valores a mano en la tabla —sin fuente, sin
fecha, sin manera de auditarlos— y el hueco se tapó sin que nadie mirase por qué existía.

No era un hueco de World Bank. **Era un join por la columna equivocada.**

De ahí sale la regla que gobierna toda la fase:

> Los países se juntan por **código**, nunca por nombre. El nombre es una etiqueta para
> mostrar; el código es la clave. En cuanto esa regla se cumple, B5 y B7 desaparecen los
> dos, y no hace falta parche ninguno.

Y de paso explica por qué B7 no se "arregla": se **borra**. Un parche cuya causa ha
desaparecido no hay que migrarlo a dbt.

---

## 2. Decisión 1 — qué es una fila de `staging`

Las dos fuentes llegan a RAW con formas muy distintas, y no por descuido: cada una guarda
*la unidad que se descargó* (la regla que se fijó en fase 2).

**`raw.openalex`: 5 filas.** Una por año. Los ~200 países viven dentro, en
`payload.group_by`, que es un array de structs (`key`, `key_display_name`, `count`).

**`raw.worldbank`: 6625 filas.** Una por registro país-indicador-año, con el envoltorio
repetido en cada línea.

Para que se puedan juntar, las dos tienen que llegar al mismo grano. La asimetría se
resuelve donde siempre se dijo que se resolvería: **la explosión es una transformación y
vive en `staging/`**, con `UNNEST` sobre el array de OpenAlex.

Lo que hay que decidir explícitamente es el grano de salida de cada modelo de staging:

- `stg_openalex`: una fila por **país-año**, con el `count` de artículos. Sale de explotar
  `group_by`.
- `stg_worldbank`: una fila por **país-año-indicador**. Es el grano que ya tiene RAW.

Y ahí aparece la segunda pregunta, que es la que de verdad importa: **¿el pivote de
indicadores (una columna por métrica) es `staging` o `intermediate`?** `staging` se define
como limpieza 1:1 desde RAW —renombrar, castear, filtrar—, y pivotar no es 1:1: cambia el
grano de país-año-indicador a país-año. Si se hace en staging, la capa deja de significar
lo que dice su nombre. Si se hace en intermediate, staging queda aburrido y honesto, que es
exactamente lo que debe ser.

## 3. Decisión 2 — la clave de join, y quién es el dueño de la lista de países

La regla de §1 dice "por código". Falta decir **qué código**, porque no hay uno solo:

- OpenAlex da **ISO2**, incrustado en una URL (`.../countries/KR`). Hay que extraerlo.
- World Bank da **las dos**: `country.id` (2 letras) y `countryiso3code` (3 letras).

Con ISO2 basta para juntar las dos fuentes. Pero hay dos trampas:

**`country.id` de World Bank no siempre es ISO2.** Los agregados usan códigos propios:
`ZH` = "Africa Eastern and Southern", `XD` = "High income". No son países y no deben entrar
en el score. La línea base los filtraba con una lista de nombres hardcodeada —frágil, y
además vuelve a juntar por nombre—. La API ofrece `region.value == 'Aggregates'`, pero
**ese campo no está en RAW**: la ingesta no lo guarda. Así que el filtro tiene que salir de
otro sitio, y el sitio natural es la dimensión.

**Alguien tiene que ser el dueño de la lista de países válidos.** Ese es el papel de
`seeds/dim_countries.csv`: un CSV versionado con una fila por país —ISO2, ISO3, nombre
canónico, `BLOC`, `IS_NATO`— que hace tres cosas a la vez: define qué es un país (todo lo
que no esté, no entra), da el nombre para mostrar (uno solo, no el de cada fuente), y
aporta el bloque para el roll-up NATO/BRICS/EU/OTHER. `pycountry` ya es dependencia del
proyecto y puede generar el CSV en vez de escribirlo a mano; lo que no puede hacer es
decidir el `BLOC`, que es una decisión editorial y por eso se versiona.

Con esa dimensión, el `relationships` de dbt deja de ser decorativo: cualquier código que
aparezca en una fuente y no esté en la dimensión **rompe el build**. Es lo contrario del
parche de Korea — un país que no casa deja de ser invisible y pasa a ser ruidoso.

## 4. Decisión 3 — materializaciones y qué se prueba

Convenciones a fijar de una vez, no modelo a modelo:

- `staging/` como **vistas**: son baratas, se leen poco, y no merece la pena materializar
  una limpieza 1:1.
- `marts/` como **tablas**: se consultan desde fuera (BI), y el coste de escanear se paga
  una vez.
- Nombres: `stg_<fuente>`, `int_<qué hace>`, `<grano>_<tema>` en marts. Sin excepciones,
  porque la excepción es la que obliga a abrir el fichero.

Y los tests, que en dbt son parte del modelo y no un extra:

- `unique` + `not_null` sobre la clave de grano de cada modelo. Es lo que impide que un
  `UNNEST` mal escrito duplique filas sin que nadie lo note — el mismo error A2 que espera
  en la fase de patentes.
- `relationships` contra `dim_countries` (§3).
- `accepted_values` sobre `BLOC`.
- Un test propio para el rango temporal: ningún año fuera de `config.YEARS`. La puerta de
  validación ya lo garantiza en la ingesta; comprobarlo también aquí es barato y cierra A3
  por los dos lados.

---

## 5. El score, en `marts/`

El briefing lo fija y esta fase lo implementa, sin reabrirlo:

- Cuatro dimensiones con peso igual (0.25) como baseline documentado.
- `GDP` y `RD_GDP_PCT` son **inputs**: entran como eje de normalización o de eficiencia
  (output/input), **nunca sumados al score**.
- Cada métrica en forma absoluta **y** relativa (per cápita / per PIB), ambas
  parametrizadas — no se elige una.
- Sin patentes todavía: esa dimensión llega en su fase. El modelo debe admitirla sin
  reescribirse, no simularla.

Aquí muere **B6**: una sola definición, en un solo fichero, versionada. Si mañana hace
falta otra vista, es un modelo que la referencia con `ref()`, no una copia con el cuerpo
cambiado.

---

## 6. Orden de build

Por dependencia, no por preferencia.

1. **Instalar `dbt-core` + `dbt-bigquery`** y crear el perfil `ai_countries` con `oauth` y
   `location: US`. `dbt debug` en verde antes de escribir un modelo.
2. **`dbt_project.yml`** con las materializaciones de §4.
3. **`sources.yml`**: declarar `raw.openalex` y `raw.worldbank` como fuentes, con
   `freshness` si procede. Los modelos leen con `source()`, nunca con el nombre de la tabla.
4. **`seeds/dim_countries.csv`** — sin esto no hay join ni filtro de agregados.
5. **`stg_openalex`** (el `UNNEST` + extraer el ISO2 de la URL) y **`stg_worldbank`**.
6. **`intermediate/`**: pivote de indicadores a país-año y join con la dimensión.
7. **`marts/`**: el score y el roll-up por bloque.
8. **Tests de dbt** en cada escalón, no al final.

---

## 7. Cómo se demuestra que la fase está cerrada

- **Korea aparece con sus cinco años de `RESEARCHERS_PER_MILLION`, sin un solo valor
  escrito a mano.** Es la prueba de B5 y B7 a la vez, y es una consulta, no una opinión.
- `dbt build` en verde: modelos y tests.
- Ningún país en las fuentes que no esté en `dim_countries` (lo garantiza
  `relationships`), y ningún agregado dentro del score.
- Una sola definición de la vista de KPIs (B6): `grep` no encuentra dos cuerpos distintos.
- El score reproduce el mismo resultado en dos ejecuciones seguidas sobre el mismo RAW.

---

## 8. Pendientes / decisiones abiertas

- [ ] **¿Pivote en `staging` o en `intermediate`?** (§2). Afecta a qué significa "staging es
      1:1" en este proyecto.
- [ ] **¿`dim_countries` generado con `pycountry` o escrito a mano?** (§3). El `BLOC` es
      editorial en cualquier caso.
- [ ] **Citas de OpenAlex** (`cited_by_count`): el briefing las quiere como métrica de
      calidad y **no están en la ingesta**. Si entran, es una reingesta, no un modelo dbt.
      Decidir si esta fase las incorpora o si el score arranca solo con volumen.
- [ ] **`dbt/` dentro del repo o al lado**: hoy el proyecto dbt viviría en `dbt/`, con el
      `pyproject.toml` de Python un nivel arriba. Conviene confirmarlo antes de que Airflow
      tenga que encontrar el `dbt_project.yml` en fase 4.
