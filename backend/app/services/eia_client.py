import time
import requests

from app.core.config import settings
from app.core.logging import setup_logger

# Logger for outbound API requests, retries, and client-level failures.
logger = setup_logger("eia_client")


# Base exception for all EIA client errors.
class EIAClientError(Exception):
    pass


# Raised when the API key is missing or rejected by the EIA API.
class EIAInvalidAPIKeyError(EIAClientError):
    pass


# Raised when the request fails after the configured retry attempts.
class EIANetworkError(EIAClientError):
    pass


# Thin client responsible for calling the EIA API and handling pagination.
class EIAClient:
    def __init__(self):
        # Load API configuration once so request methods stay small and focused.
        self.base_url = settings.EIA_BASE_URL.rstrip("/")
        self.dataset_path = settings.EIA_DATASET_PATH.strip("/")
        self.api_key = settings.EIA_API_KEY
        self.data_columns = settings.EIA_DATA_COLUMNS

    # Build the dataset or dataset/data endpoint depending on the requested suffix.
    def _build_url(self, suffix: str | None = "data") -> str:
        if suffix:
            return f"{self.base_url}/{self.dataset_path}/{suffix}"
        return f"{self.base_url}/{self.dataset_path}"

    # Centralize request execution with basic retry handling and API key validation.
    def _request_with_retry(self, url: str, params, retries: int = 1) -> dict:
        last_error = None

        for attempt in range(retries + 1):
            try:
                response = requests.get(url, params=params, timeout=60)

                # Treat authorization failures as a dedicated configuration error.
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

                # Wait briefly before retrying transient request failures.
                if attempt < retries:
                    time.sleep(1)

        raise EIANetworkError(f"Request failed after retry: {last_error}")

    # Fetch dataset-level metadata from the EIA API.
    def get_dataset_metadata(self) -> dict:
        if not self.api_key:
            raise EIAInvalidAPIKeyError(
                "EIA_API_KEY is missing. Please set it in your .env file."
            )

        url = self._build_url(None)
        params = {"api_key": self.api_key}

        logger.info("Requesting dataset metadata")
        return self._request_with_retry(url=url, params=params, retries=1)

    # Fetch one page of outage data using offset-based pagination.
    def get_data_page(
        self,
        length: int = 5,
        offset: int = 0,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        if not self.api_key:
            raise EIAInvalidAPIKeyError(
                "EIA_API_KEY is missing. Please set it in your .env file."
            )

        url = self._build_url("data")

        # Keep request parameters explicit so the selected columns and sort order
        # are predictable across refresh runs.
        params = [
            ("api_key", self.api_key),
            ("offset", str(offset)),
            ("length", str(length)),
        ]

        if start_date:
            params.append(("start", start_date))

        if end_date:
            params.append(("end", end_date))

        params.extend(
            [
                ("sort[0][column]", "period"),
                ("sort[0][direction]", "desc"),
                ("sort[1][column]", "facility"),
                ("sort[1][direction]", "asc"),
            ]
        )

        # Request only the columns needed by the ingestion pipeline.
        for column in self.data_columns:
            params.append(("data[]", column))

        logger.info(
            "Requesting EIA data page | offset=%s length=%s start_date=%s end_date=%s data_columns=%s",
            offset,
            length,
            start_date,
            end_date,
            self.data_columns,
        )

        return self._request_with_retry(url=url, params=params, retries=1)

    # Read the full dataset by iterating through all available pages.
    def get_all_data(
        self,
        page_size: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict]:
        page_size = page_size or settings.DEFAULT_PAGE_SIZE
        offset = 0
        all_rows = []
        total = None

        while True:
            response = self.get_data_page(
                length=page_size,
                offset=offset,
                start_date=start_date,
                end_date=end_date,
            )
            response_block = response.get("response", {})
            page_data = response_block.get("data", [])

            # Capture the reported total once to know when pagination is complete.
            if total is None:
                total = int(response_block.get("total", 0))
                logger.info("API total rows available for current query: %s", total)

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

            # Stop once the offset reaches the total rows reported by the API.
            if total and offset >= total:
                logger.info("Reached API total using offset pagination.")
                break

        return all_rows