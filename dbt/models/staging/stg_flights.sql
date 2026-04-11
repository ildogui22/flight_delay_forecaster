with source as (
    select * from raw.flights
),

renamed as (
    select
        icao24,
        callsign,
        "estDepartureAirport"   as departure_airport,
        "estArrivalAirport" as arrival_airport,
        "firstSeen_ts" as departed_at,
        "lastSeen_ts" as arrived_at,
        duration_minutes,
        year,
        month
    from source
    where icao24 is not null
    and "firstSeen_ts" is not null
    and duration_minutes > 0
    and "estArrivalAirport" in ('EDDF', 'EGLL', 'LFPG', 'EHAM')
    and "estDepartureAirport" != "estArrivalAirport"
)

select * from renamed