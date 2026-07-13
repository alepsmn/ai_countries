AI Countries — Briefing de reconstrucción

Objetivo

Endurecer un pipeline analítico existente hacia grado portfolio Data Engineering junior.
El entregable que se vende es el pipeline (ELT idempotente, tipado, testeado, orquestado), no el modelo.
Output analítico: score compuesto ponderado que clasifica países y bloques (NATO/BRICS/EU/OTHER) por capacidad en IA.

NO es un proyecto de ML. Score determinista y auditable, sin clustering ni sklearn.

Soy estudiante de Ingenieria y Sistemas de Datos (3o UPM) estoy creando un CV para buscar las mejores practicas posibles
para el siguiente curso y posteriormente entrar como Data Engineer a trabajar y luego pivotar a Machine Learning Engineer o
AI Engineer, entre las herramientas que he dado: matematicas de ingenieria, probabilidad y senales aleatorias, optimizacion,
inferencia estadistica y variacional (VAEs, GANs, un trabajo serio sobre demostrar si la jerarquia en VAEs es importante
para detectar estructuras complejas y ordenadas en retinopatias -> NVAEs) y de infra: docker, k8s, kafka, spark...
He hecho un proyecto batch serio con tests, type hints etc, estoy terminando otro, pero necesito ahora un ejemplo real de produccion
para un proyecto streaming, del que pueda aprender, del que tomar lecciones.
A partir de los batch he tocado algo de BigQuery, dbt, airflow, pero sigo aprendiendo

Estos son los roadmap que he seguido:
Roadmap de Python para tu objetivo (DE→MLE, no Python genérico). Ordenado por dependencia, no por preferencia. Tu gap no es sintaxis: es endurecer Python hacia producción. Eso fija la secuencia.

Modelo de datos y ejecución. Referencias vs copias, mutabilidad, identidad vs igualdad, scope/closures, GIL. Sustrato. Sin esto memorizas reglas en vez de entender comportamientos. Todo lo demás depende de aquí.
Estructuras idiomáticas y coste algorítmico. dict/set/list/tuple, comprehensions, generadores, iteradores, módulo collections. Coste real de cada operación. Es donde un script y código que procesa volúmenes divergen. Crítico en DE.
Tipado estático y contratos. Type hints, mypy/pyright, dataclasses, Protocol, TypedDict. Producción exige contratos verificables. Señal directa del engineering que declaras faltarte.
Errores y robustez. Excepciones, context managers, logging (no print). Idempotencia y retries que verás en Airflow presuponen esto. Dependencia hacia adelante.
Concurrencia y paralelismo. threading vs multiprocessing vs asyncio. Aquí el GIL del punto 1 se vuelve decisión: I/O-bound vs CPU-bound. No antes: sin el modelo de ejecución es cargo cult.
Testing. pytest, fixtures, mocking, parametrización. No negociable en producción. Es el umbral júnior→contratable.
Packaging y entorno. Módulos, imports, estructura de proyecto, venv/uv. Último porque empaqueta lo anterior.

Justificación del orden: 1 explica 5. 4 habilita Airflow. 2 es el cuello de botella en DE. 3 y 6 son la señal de engineering que tu CV necesita declarar con sustancia, no con palabra

--

Premisa: tu objetivo es DE junior, no analista. El SQL que importa es el que aparece en entrevistas técnicas y en el trabajo real.

Nivel 0 — Base (ya tienes esto)

JOINs, GROUP BY, WHERE, subconsultas simples. No dedicar tiempo aquí.

Nivel 1 — Window Functions (2 semanas)

Esto es lo que separa junior de nada en entrevistas DE.

ROW_NUMBER, RANK, DENSE_RANK
LAG, LEAD
SUM/AVG OVER (PARTITION BY ... ORDER BY ...)
Frames: ROWS BETWEEN

Recurso: Leetcode SQL Medium gratuitas filtradas por window functions. Mínimo 15 problemas.

Nivel 2 — CTEs y lógica compleja (2 semanas)

CTEs encadenadas (WITH a AS (...), b AS (...))
CTEs recursivas (jerarquías, series)
Gaps and islands (series temporales con huecos)

Recurso: tus propios datos de BQ + Leetcode Hard.

Nivel 3 — Performance y warehouse (1 semana)

Específico de BQ, diferenciador real en CV:

Particionado y clustering
Coste de bytes escaneados
UNNEST para arrays y structs
Evitar SELECT *

Recurso: documentación oficial de BigQuery. No hay atajos aquí.

Nivel 4 — Lo que se pregunta en entrevistas DE (1 semana)

Patrones recurrentes:

Sessionization (agrupar eventos en sesiones)
Deduplicación con ROW_NUMBER
Pivoting sin PIVOT nativo
Running totals

Recurso: StrataScratch empresa = Airbnb, Meta, Spotify (los que tienen más preguntas DE gratuitas).

El objetivo es obtener un CV mucho mas competente que la media, y no buscar DE con stack basico (SQL, poco mas)


Stack objetivo (decidido, no negociable)


Warehouse: BigQuery. (Antes Snowflake — se descarta: las patentes ya viven en BQ, mover BQ→Snowflake añade frontera de datos sin beneficio.)
Landing: GCS (raw en parquet/jsonl).
Transformación: dbt-core (no dbt Cloud).
Orquestación: Airflow, invocando dbt-core. (Antes Apache Hop — se descarta.)
Lenguaje ingesta: Python con type hints + mypy/pyright, tests con pytest.


Fuentes de datos


OpenAlex (API): artículos IA por país/año + citas recibidas (cited_by_count, normalizadas por año — pendiente de integrar, no estaba en la línea base).
World Bank (API): POPULATION, GDP_USD, GDP_PER_CAPITA_USD, RD_GDP_PCT, RESEARCHERS_PER_MILLION.
Google Patents (patents-public-data.patents.publications en BQ): patentes IA vía CPC G06N%.


Modelo del score (a construir en marts/, no existe aún)

Cuatro dimensiones ortogonales, seis métricas base, dos derivadas:


Producción: volumen (nº artículos) + calidad (citas norm. por año). Ortogonales, no fundir.
Traslación: patentes IA + ratio patentes/artículos (eficiencia traslacional).
Capital humano: graduados/investigadores STEM per cápita (proxy grueso — declarar como tal, STEM≠IA).
Corrección de tamaño: regla transversal. Cada métrica en forma absoluta y relativa (per cápita / per PIB).


Reglas de coherencia del score:


GDP y RD_GDP_PCT son INPUTS, no componentes del score de output. Entran solo como ejes de normalización o eje de eficiencia (output/input). NUNCA sumados al score. (No se cometió el error en la línea base — es preventivo.)
Absoluto vs relativo: parametrizar ambas salidas, no elegir. Peso configurable.
Pesos entre dimensiones: iguales (0.25) como baseline documentado. Cualquier otro reparto exige justificación explícita.


Estado de la línea base (código previo)

Existe ingesta funcional + una vista pivote (VW_AI_KPI). No hay score todavía.
El código previo es material a endurecer, no a preservar. Lo que funcione se mantiene; lo demás se corrige.


ERRORES A CORREGIR (ordenados por severidad)

A. Corrupción de datos — máxima prioridad


country_code en patentes = jurisdicción de la oficina, no nacionalidad del inventor. Infla US/EPO. Usar inventor_harmonized.country_code (o assignee_harmonized). Cambia los números.
Doble conteo por UNNEST(cpc). COUNT(*) cuenta una fila por cada código G06N. Usar COUNT(DISTINCT publication_number).
Rango temporal inconsistente. Patentes 2019–2024, OpenAlex 2019–2023, WB 2019–2024, vista 2019–2023. Definir una constante única de rango propagada a todas las extracciones.


B. Arquitectura DE — rompe en Airflow


Sin idempotencia. Ingesta hace append (write_pandas, executemany INSERT). Un retry de Airflow duplica datos. Reemplazar por MERGE por clave (COUNTRY_ID, YEAR, INDICATOR) o DELETE-partition + INSERT.
Modelo EAV desnormalizado. COUNTRY_NAME repetido en cada tabla RAW → causa el bug de "Korea, Republic of" vs "Korea, Rep." y sus 3 UPDATEs manuales. RAW guarda solo COUNTRY_ID; el nombre se resuelve por join contra la dimensión.
VW_AI_KPI definida 3 veces con cuerpos distintos. En dbt: un modelo, versionado, fuente única de verdad.
Parche manual de Korea (5 valores hardcoded). Diagnosticar causa raíz en World Bank antes de hardcodear. Valor sin fuente ni fecha = deuda no auditable.


C. Robustez — obligatorio antes de orquestar


requests.get sin timeout=. Añadir timeout + retries con backoff.
OpenAlex: r.json() sin verificar status_code (WB sí lo hace — incoherencia). Verificar antes de parsear.
Rutas absolutas C:\Users\alex\Desktop\... hardcodeadas. No existen en Airflow. Config por variable de entorno.
except: desnudo en iso2_to_country. Usar excepción específica.


Se conserva


Paginación de World Bank (correcta).
Filtro de agregados WB — pero mejorar: filtrar por region.value == 'Aggregates' de la API en vez de lista hardcoded frágil.
Separación RAW/ANALYTICS = germen correcto de staging/marts.
DIM_COUNTRIES (BLOC/IS_NATO) = dimensión para roll-up por bloque. Migra a seeds/ en dbt.



DECISIONES BLOQUEANTES — preguntar al usuario ANTES de tocar ingesta

No asumir defaults en estos tres puntos:


[BLOQUEANTE] Conteo de coautoría OpenAlex. group_by: authorships.countries hace full counting (paper US-China suma +1 a ambos). Alternativa: fractional counting (1/n). Full infla colaboración internacional; fractional reparte crédito. Sin opción neutra. Confirmar cuál y por qué.
[BLOQUEANTE] concepts.id:C154945302 está deprecado. OpenAlex migró concepts→topics. El concept responde pero está congelado. ¿Mantener por continuidad o migrar a topics y reingesta? Afecta comparabilidad temporal.
[BLOQUEANTE] Diagnóstico Korea. Query a WB (KOR / RESEARCHERS_PER_MILLION / 2019–2023) para hallar la causa del hueco antes de decidir el fix definitivo. No replicar el hardcode.



Estructura de repo objetivo (a crear)

ai_countries/
├── ingestion/          # Python tipado, tests
│   ├── openalex.py
│   ├── worldbank.py
│   ├── patents.py
│   └── config.py       # rango temporal, rutas por env var
├── dbt/
│   ├── models/
│   │   ├── staging/    # limpieza 1:1 desde RAW
│   │   ├── intermediate/
│   │   └── marts/      # score compuesto + roll-up por bloque + dims
│   ├── seeds/          # DIM_COUNTRIES
│   └── tests/          # tests dbt (not_null, unique, relationships, accepted_values)
├── airflow/
│   └── dags/
├── tests/              # pytest sobre ingestion/
├── pyproject.toml      # deps + config mypy/pytest
└── .env.example

Orden de build (dependencia, no preferencia)


Resolver 3 bloqueantes.
config.py (rango temporal único + env vars). Elimina errores 3 y 10 de raíz.
Ingesta tipada + idempotente (MERGE) + robusta (timeout, status check). Errores 1,2,4,8,9,11.
Tests pytest sobre ingesta.
dbt: staging → intermediate → marts. Normaliza (error 5), unifica vista (error 6), construye score.
Tests dbt.
Airflow DAG orquestando 3→5.
(Opcional) Power BI o Metabase sobre marts. No prioritario.

Como el objetivo es aprender, yo ejecuto todo, tu me guias, y vas creando como un pre-readme detallado por cada paso grande - fase , para luego sintetizarlo en el propio README

DataSet patentes
SELECT
    country_code,
    CAST(SUBSTR(CAST(publication_date AS STRING), 1, 4) AS INT64) AS year,
    COUNT(*) AS ai_patents
FROM `patents-public-data.patents.publications`,
    UNNEST(cpc) AS cpc_code
WHERE
    cpc_code.code LIKE 'G06N%'
    AND CAST(SUBSTR(CAST(publication_date AS STRING), 1, 4) AS INT64) BETWEEN 2019 AND 2024
    AND country_code IS NOT NULL
    AND country_code != ''
GROUP BY country_code, year
ORDER BY year, ai_patents DESC