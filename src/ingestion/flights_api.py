import os
from dataclasses import asdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from opensky_api import OpenSkyApi

load_dotenv()

def _get_client() -> OpenSkyApi:
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

    if client_id and client_secret:
        return OpenSkyApi(client_id=client_id, client_secret=client_secret)
    else:
        return OpenSkyApi()
    
def fetch_departures(
        airport: str,
        start_date: str,
        end_date: str
) -> list[dict]:
    
    begin = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    with _get_client() as api:
        flights = api.get_departures_by_airport(airport, begin, end)

    if flights is None:
        return []
    else:
        return [vars(f) for f in flights]


if __name__ == "__main__":
    import json

    data = fetch_departures(
        airport="EDDF",
        start_date="2024-01-01",
        end_date="2024-01-02",
    )


    with open("src/ingestion/sample_flights.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} flights to sample_flights.json")
