with aq as (
    select * from {{ ref('stg_air_quality') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

joined as (
    select
        aq.city,
        aq.date,

        -- raw air quality
        aq.pm25,
        aq.pm10,
        aq.o3,
        aq.no2,
        aq.co,

        -- pm25 features
        lag(aq.pm25, 1) over (partition by aq.city order by aq.date) as pm25_lag_1d,
        lag(aq.pm25, 7) over (partition by aq.city order by aq.date) as pm25_lag_7d,
        avg(aq.pm25) over (partition by aq.city order by aq.date rows between 6 preceding and current row) as pm25_rolling_7d,

        -- pm10 features
        lag(aq.pm10, 1) over (partition by aq.city order by aq.date) as pm10_lag_1d,
        lag(aq.pm10, 7) over (partition by aq.city order by aq.date) as pm10_lag_7d,
        avg(aq.pm10) over (partition by aq.city order by aq.date rows between 6 preceding and current row) as pm10_rolling_7d,

        -- o3 features
        lag(aq.o3, 1) over (partition by aq.city order by aq.date) as o3_lag_1d,
        lag(aq.o3, 7) over (partition by aq.city order by aq.date) as o3_lag_7d,
        avg(aq.o3) over (partition by aq.city order by aq.date rows between 6 preceding and current row) as o3_rolling_7d,

        -- no2 features
        lag(aq.no2, 1) over (partition by aq.city order by aq.date) as no2_lag_1d,
        lag(aq.no2, 7) over (partition by aq.city order by aq.date) as no2_lag_7d,
        avg(aq.no2) over (partition by aq.city order by aq.date rows between 6 preceding and current row) as no2_rolling_7d,

        -- co features
        lag(aq.co, 1) over (partition by aq.city order by aq.date) as co_lag_1d,
        lag(aq.co, 7) over (partition by aq.city order by aq.date) as co_lag_7d,
        avg(aq.co) over (partition by aq.city order by aq.date rows between 6 preceding and current row) as co_rolling_7d,

        -- weather features
        w.temperature_c,
        w.humidity_pct,
        w.wind_speed_kmh,
        w.wind_gusts_kmh,
        w.wind_direction_deg,
        w.precipitation_mm,
        w.snowfall_cm,
        w.snow_depth_m,
        w.cloud_cover_pct,
        w.surface_pressure_hpa,
        w.weather_code

    from aq
    left join weather w
        on aq.city = w.city
        and aq.date = w.date
)

select * from joined
