with features as (
    select * from {{ ref('int_flight_features') }}
),

final as (
    select
        icao24,
        departure_airport,
        arrival_airport,
        duration_minutes,

        dep_temperature_c,
        dep_humidity_pct,
        dep_wind_speed_kmh,
        dep_precipitation_mm,
        dep_weather_code,

        arr_temperature_c,
        arr_humidity_pct,
        arr_wind_speed_kmh,
        arr_precipitation_mm,
        arr_weather_code,
        
        dep_wind_gusts_kmh, dep_wind_direction_deg,
        dep_snowfall_cm, dep_snow_depth_m,
        dep_cloud_cover_pct, dep_surface_pressure_hpa,
        arr_wind_gusts_kmh, arr_wind_direction_deg,
        arr_snowfall_cm, arr_snow_depth_m,
        arr_cloud_cover_pct, arr_surface_pressure_hpa,

        year,
        month
    from features
    where duration_minutes is not null
    and dep_temperature_c is not null
    and arr_temperature_c is not null
)

select * from final