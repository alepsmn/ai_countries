with source as (
    select *
    from {{ source('raw', 'openalex') }} as oa
),

unnested as (
    select
        source.year as year,
        source.ingested_at as ingested_at,
        REGEXP_EXTRACT(g.key, r'countries/([A-Z]{2})$') as country_code,
        -- avisar de que viene de OA para evitar confusiones con otras fuentes
        g.key_display_name as country_name_oa,
        g.count as articles
    -- la coma es un cross join, la segunda tabla depende de la primera
    -- para cada fila se despliega solo su propio array 
    -- ej 2019 x 200 paises
    from source, UNNEST(source.payload.group_by) as g
)

select * from unnested