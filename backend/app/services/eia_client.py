import time
import requests

from app.core.config import settings
from app.core.logging import setup_logger

logger = setup_logger("eia_client")


class EIAClientError(Exception):
    pass


class EIAInvalidAPIKeyError(EIAClientError):
    pass


class EIANetworkError(EIAClientError):
    pass


class EIAClient:
    def __init__(self):
        self.base_url = settings.EIA_BASE_URL.rstrip("/")
        self.dataset_path = settings.EIA_DATASET_PATH.strip("/")
        self.api_key = settings.EIA_API_KEY
        self.data_columns = settings.EIA_DATA_COLUMNS

    def _build_url(self, suffix: str | None = "data") -> str:
        if suffix:
            return f"{self.base_url}/{self.dataset_path}/{suffix}"
        return f"{self.base_url}/{self.dataset_path}"

    def _request_with_retry(self, url: str, params, retries: int = 1) -> dict:
        last_error = None

        for attempt in range(retries + 1):
            try:
                response = requests.get(url, params=params, timeout=20)

                if response.status_code in (401, 403):
                    raise EIAInvalidAPIKeyError(
                        "Invalid EIA API key. Please verify your EIA_API_KEY."
                    )

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as exc:
                last_error = exc
                logger.warning(
                    "Request failed on attempt %s/%s: %s",
                    attempt + 1,
                    retries + 1,
                    exc,
                )

                if attempt < retries:
                    time.sleep(1)

        raise EIANetworkError(f"Request failed after retry: {last_error}")

    def get_dataset_metadata(self) -> dict:
        if not self.api_key:
            raise EIAInvalidAPIKeyError(
                "EIA_API_KEY is missing. Please set it in your .env file."
            )

        url = self._build_url(None)
        params = {"api_key": self.api_key}

        logger.info("Requesting dataset metadata")
        return self._request_with_retry(url=url, params=params, retries=1)

    def get_data_page(
        self,
        length: int = 5,
        offset: int = 0,
    ) -> dict:
        if not self.api_key:
            raise EIAInvalidAPIKeyError(
                "EIA_API_KEY is missing. Please set it in your .env file."
            )

        url = self._build_url("data")

        params = [
            ("api_key", self.api_key),
            ("offset", str(offset)),
            ("length", str(length)),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "desc"),
            ("sort[1][column]", "facility"),
            ("sort[1][direction]", "asc"),
        ]

        for column in self.data_columns:
            params.append(("data[]", column))

        logger.info(
            "Requesting EIA data page | offset=%s length=%s data_columns=%s",
            offset,
            length,
            self.data_columns,
        )

        return self._request_with_retry(url=url, params=params, retries=1)

    def get_all_data(self, page_size: int | None = None) -> list[dict]:
        page_size = page_size or settings.DEFAULT_PAGE_SIZE
        offset = 0
        all_rows = []
        total = None

        while True:
            response = self.get_data_page(length=page_size, offset=offset)
            response_block = response.get("response", {})
            page_data = response_block.get("data", [])

            if total is None:
                total = int(response_block.get("total", 0))
                logger.info("API total rows available: %s", total)

            if not page_data:
                logger.info("No more rows returned by API. Stopping pagination.")
                break

            all_rows.extend(page_data)

            logger.info(
                "Fetched page successfully | offset=%s page_rows=%s accumulated_rows=%s",
                offset,
                len(page_data),
                len(all_rows),
            )

            offset += page_size

            if total and offset >= total:
                logger.info("Reached API total using offset pagination.")
                break

        return all_rows
