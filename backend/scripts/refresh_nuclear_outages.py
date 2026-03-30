# Script entry point for refreshing nuclear outage data from the EIA API
from app.core.logging import setup_logger
from app.services.eia_client import EIAClientError, EIAInvalidAPIKeyError
from app.services.refresh_service import run_refresh

# Logger used to track refresh execution and failures
logger = setup_logger("refresh_script")


def main():
    try:
        # Run the full refresh pipeline: fetch, validate, transform, and store
        result = run_refresh()
        logger.info("Refresh result: %s", result)

    except EIAInvalidAPIKeyError as exc:
        logger.error(str(exc))

    except EIAClientError as exc:
        logger.error("EIA client error: %s", exc)

    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)


if __name__ == "__main__":
    main()