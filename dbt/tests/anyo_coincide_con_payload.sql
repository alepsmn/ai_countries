-- Test singular: la anotacion de la ingesta y el dato de la fuente dicen lo mismo.
--
-- `year` lo escribe nuestra ingesta al aterrizar el fichero; `payload.date` es lo
-- que dijo World Bank. Hoy coinciden en las 6625 filas, asi que da igual cual se
-- use --- y eso es exactamente lo que conviene congelar. `stg_worldbank` se queda
-- con `year` (ya es INT64 y es la columna que valida la puerta de ingesta); si
-- algun dia la anotacion y el payload divergen, se quiere saber por un build rojo
-- y no por un score raro.
--
-- Un test pasa cuando no devuelve filas.

select
    wb.year as year_anotado,
    wb.payload.date as date_payload,
    count(*) as filas
from {{ source('raw', 'worldbank') }} as wb
where wb.year <> cast(wb.payload.date as int64)
group by year_anotado, date_payload
