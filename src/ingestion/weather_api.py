import requests
import airportsdata

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

DEFAULT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "precipitation",
    "weather_code",
]

def get_airport_coords(icao: str) -> tuple[float, float] | None:
    airports = airportsdata.load("ICAO")
    airport = airports.get(icao)
    if airport is None:
        return None
    return (airport["lat"], airport["lon"])

def fetch_weather(
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        variables: list = DEFAULT_VARIABLES
) -> dict:
    
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


if __name__ == "__main__":
    import json

    data = fetch_weather(
        latitude=48.85,
        longitude=2.35,
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    with open("sample_response.json", "w") as f:
        json.dump(data, f, indent=2)

    print("Saved to sample_response.json")