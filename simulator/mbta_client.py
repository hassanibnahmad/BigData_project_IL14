
import os
import requests
from dotenv import load_dotenv

load_dotenv()


class MBTAClient:
    def __init__(self):
        self.base_url = os.getenv(
            "MBTA_API_URL",
            "https://api-v3.mbta.com"
        ).rstrip("/")

        self.routes = [
            route.strip()
            for route in os.getenv(
                "MBTA_ROUTES", "1,66,77"
            ).split(",")
            if route.strip()
        ]

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.api+json"
        })

        api_key = os.getenv("MBTA_API_KEY")
        if api_key:
            self.session.headers["x-api-key"] = api_key

    def get_vehicles(self):
        response = self.session.get(
            f"{self.base_url}/vehicles",
            params={
                "filter[route]": ",".join(self.routes),
                "page[limit]": 100
            },
            timeout=20
        )

        response.raise_for_status()
        payload = response.json()

        return payload.get("data", [])
