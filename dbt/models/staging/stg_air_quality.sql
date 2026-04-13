with source as (
    select * from raw.air_quality
),

renamed as (
    select
        city,
        date,
        pm25,
        pm10,
        o3,
        no2,
        co
    from source
    where city is not null
      and date is not null
)

select * from renamed
