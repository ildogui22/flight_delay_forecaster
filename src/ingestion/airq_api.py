import os
import requests
from dotenv import load_dotenv

from config import CITIES


load_dotenv()

WAQI_API_KEY = os.getenv("WAQI_API_KEY")
WAQI_BASE_URL = os.getenv("WAQI_BASE_URL", "https://api.waqi.info")


def fetch_air_quality(city: str) -> dict:
    """
    Fetch daily air quality reading for a city from WAQI.
    Returns a dict with keys: date, city, pm25, pm10, o3, no2, co.
    Raises ValueError if no data returned.
    """
    
    station_id = CITIES[city]["station_id"]
    url = f"{WAQI_BASE_URL}/feed/@{station_id}/"
    response = requests.get(url, params={"token": WAQI_API_KEY}, timeout=30)
    response.raise_for_status()

    body = response.json()
    if body.get("status") != "ok":
        raise ValueError(f"WAQI error for {city}: {body.get('data')}")
    
    iaqi = body["data"]["iaqi"]
    date = body["data"]["time"]["s"][:10]

    return {
    "date":  date,
    "city":  city,
    "pm25":  iaqi.get("pm25", {}).get("v"),
    "pm10":  iaqi.get("pm10", {}).get("v"),
    "o3":    iaqi.get("o3",   {}).get("v"),
    "no2":   iaqi.get("no2",  {}).get("v"),
    "co":    iaqi.get("co",   {}).get("v"),
}
