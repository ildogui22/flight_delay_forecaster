with source as (
    select * from raw.weather
),

renamed as (
    select
        airport,
        timestamp               as observed_at,
        temperature_2m          as temperature_c,
        relative_humidity_2m    as humidity_pct,
        wind_speed_10m          as wind_speed_kmh,
        wind_gusts_10m          as wind_gusts_kmh,
        wind_direction_10m      as wind_direction_deg,
        precipitation           as precipitation_mm,
        snowfall                as snowfall_cm,
        snow_depth              as snow_depth_m,
        cloud_cover             as cloud_cover_pct,
        surface_pressure        as surface_pressure_hpa,
        weather_code,
        year,
        month
    from source
    where airport is not null
      and timestamp is not null
)

select * from renamed