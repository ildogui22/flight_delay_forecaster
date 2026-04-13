with source as (
    select * from raw.weather
),

renamed as (
    select
        city,
        date,
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
        weather_code
    from source
    where city is not null
      and date is not null
)

select * from renamed
