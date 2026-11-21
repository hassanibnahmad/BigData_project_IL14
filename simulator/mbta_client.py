
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


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

    def iter_stop_time_pages(self, endpoint, **filters):
        """Follow JSON:API next links without forwarding credentials off-origin."""
        url = f"{self.base_url}/{endpoint}"
        page_limit = 10000 if endpoint == "schedules" else 1000
        params = {"filter[route]": ",".join(self.routes), "page[limit]": page_limit, **filters}
        visited = set()
        while url:
            if urlsplit(url)[:2] != urlsplit(self.base_url)[:2] or url in visited:
                raise ValueError("Invalid MBTA pagination link")
            visited.add(url)
            payload = self._get_stop_time_page(url, params)
            yield payload
            link = (payload.get("links") or {}).get("next")
            if isinstance(link, dict):
                link = link["href"]
            url = urljoin(url, link) if link else None
            params = None

    def _get_stop_time_page(self, url, params):
        for attempt in range(4):
            delay = 2 ** attempt
            try:
                response = self.session.get(
                    url, params=params, timeout=20, allow_redirects=False
                )
                if response.status_code in (429, 500, 502, 503, 504):
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            seconds = int(retry_after)
                        except ValueError:
                            try:
                                seconds = (
                                    parsedate_to_datetime(retry_after)
                                    - datetime.now(timezone.utc)
                                ).total_seconds()
                            except (TypeError, ValueError, OverflowError):
                                seconds = 0
                        delay = max(delay, seconds)
                    if response.status_code == 429 and not retry_after:
                        delay = max(delay, 15 * (2 ** attempt))
                    if response.status_code == 429:
                        logger.warning("MBTA rate limit | HTTP 429 | retry delay %.1fs", delay)
                    response.raise_for_status()
                if 300 <= response.status_code < 400:
                    raise ValueError("Unexpected MBTA redirect")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload.get("data"), list):
                    raise ValueError("MBTA response has no data array")
                return payload
            except requests.HTTPError as error:
                if error.response.status_code not in (429, 500, 502, 503, 504):
                    raise
                if attempt == 3:
                    # Keep the cooldown in effect across polling cycles too.
                    time.sleep(delay)
                    raise
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 3:
                    raise
            time.sleep(delay)
