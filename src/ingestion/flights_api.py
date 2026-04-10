import os
from dataclasses import asdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from opensky_api import OpenSkyApi

load_dotenv()

def _get_client() -> OpenSkyApi:
    """
    Defining flights API connection
    """
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
    """
    Fetches all departures from midnight of start_date to midnight of end_date (exclusive).
    Pass end_date = start_date + 1 day to fetch a full single day.
    """
    
    begin = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    with _get_client() as api:
        flights = api.get_departures_by_airport(airport, begin, end)

    if flights is None:
        return []
    else:
        return [vars(f) for f in flights]


# Testing the response of the flights API
if __name__ == "__main__":
    import json

    data = fetch_departures(
        airport="EDDF",
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    out_path = os.path.join(os.path.dirname(__file__), "sample_flights.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} flights to sample_flights.json")
