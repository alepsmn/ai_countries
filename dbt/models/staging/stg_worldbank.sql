with source as (
    select *
    from {{ source('raw', 'worldbank') }} as wb
),

tipado as (
    select
        source.year as year,
        source.ingested_at as ingested_at,
        source.payload.indicator.id as indicator_id,
        source.payload.indicator.value as indicator_name,
        source.payload.country.id as country_code,
        source.payload.countryiso3code as country_iso3,
        source.payload.value as indicator_value,
        source.payload.country.value as country_name_wb

    from source
)

select * from tipado