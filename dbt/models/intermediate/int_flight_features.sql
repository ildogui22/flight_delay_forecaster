with flights as (
    select * from {{ ref('stg_flights') }}
),

weather_dep as (
    select * from {{ ref('stg_weather') }}
),

weather_arr as (
    select * from {{ ref('stg_weather') }}
),


joined as (
    select
        f.icao24,
        f.callsign,
        f.departure_airport,
        f.arrival_airport,
        f.departed_at,
        f.arrived_at,
        f.duration_minutes,
        f.year,
        f.month,

        -- departure weather
        w_dep.temperature_c         as dep_temperature_c,
        w_dep.humidity_pct          as dep_humidity_pct,
        w_dep.wind_speed_kmh        as dep_wind_speed_kmh,
        w_dep.wind_gusts_kmh        as dep_wind_gusts_kmh,
        w_dep.wind_direction_deg    as dep_wind_direction_deg,
        w_dep.precipitation_mm      as dep_precipitation_mm,
        w_dep.snowfall_cm           as dep_snowfall_cm,
        w_dep.snow_depth_m          as dep_snow_depth_m,
        w_dep.cloud_cover_pct       as dep_cloud_cover_pct,
        w_dep.surface_pressure_hpa  as dep_surface_pressure_hpa,
        w_dep.weather_code          as dep_weather_code,

        -- arrival weather
        w_arr.temperature_c         as arr_temperature_c,
        w_arr.humidity_pct          as arr_humidity_pct,
        w_arr.wind_speed_kmh        as arr_wind_speed_kmh,
        w_arr.wind_gusts_kmh        as arr_wind_gusts_kmh,
        w_arr.wind_direction_deg    as arr_wind_direction_deg,
        w_arr.precipitation_mm      as arr_precipitation_mm,
        w_arr.snowfall_cm           as arr_snowfall_cm,
        w_arr.snow_depth_m          as arr_snow_depth_m,
        w_arr.cloud_cover_pct       as arr_cloud_cover_pct,
        w_arr.surface_pressure_hpa  as arr_surface_pressure_hpa,
        w_arr.weather_code          as arr_weather_code

    from flights f

    left join weather_dep w_dep
        on f.departure_airport = w_dep.airport
        and date_trunc('hour', f.departed_at) = w_dep.observed_at

    left join weather_arr w_arr
        on f.arrival_airport = w_arr.airport
        and date_trunc('hour', f.arrived_at) = w_arr.observed_at
)

select * from joined