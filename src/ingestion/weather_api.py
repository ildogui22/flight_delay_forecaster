import requests
import os
from dotenv import load_dotenv

load_dotenv()

WEATHER_URL = os.getenv("WEATHER_URL")

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

def fetch_weather(
        latitude: str,
        longitude: str,
        start_date: str,
        end_date: str,
        variables: list = DEFAULT_VARIABLES
) -> dict:
    """
    Fetching weather conditions (specified variables) from OpenMeteo API
    """

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(variables),
        "timezone": "UTC",
    }

    response = requests.get(WEATHER_URL, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


# Testing the response of the weather API
if __name__ == "__main__":
    import json

    data = fetch_weather(
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    out_path = os.path.join(os.path.dirname(__file__), "sample_weather.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved to {out_path}")
