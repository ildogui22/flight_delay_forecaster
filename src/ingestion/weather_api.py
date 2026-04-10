import requests
import airportsdata
import os

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

DEFAULT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "precipitation",
    "snowfall",
    "snow_depth",
    "cloud_cover",
    "surface_pressure",
    "weather_code",
]


def get_airport_coords(icao: str) -> tuple[float, float] | None:
    """
    Get airport coordinates using icao codes from airportsdata library
    """
    airports = airportsdata.load("ICAO")
    airport = airports.get(icao)
    if airport is None:
        return None
    return (airport["lat"], airport["lon"])

def fetch_weather(
        airport: str,
        start_date: str,
        end_date: str,
        variables: list = DEFAULT_VARIABLES
) -> dict:
    """
    Fetching weather conditions (specified variables) from OpenMeteo API
    """

    coords = get_airport_coords(airport)
    if coords is None:
        print(f"No coordinates found for airport: {airport}, skipping")
        return {}
    latitude, longitude = coords

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(variables),
        "timezone": "UTC",
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


# Testing the response of the weather API
if __name__ == "__main__":
    import json

    data = fetch_weather(
        airport="EDDF",
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    out_path = os.path.join(os.path.dirname(__file__), "sample_weather.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved to {out_path}")
