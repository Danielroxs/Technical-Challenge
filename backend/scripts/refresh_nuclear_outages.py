from app.core.logging import setup_logger
from app.services.eia_client import EIAClientError, EIAInvalidAPIKeyError
from app.services.refresh_service import run_refresh

logger = setup_logger("refresh_script")


def main():
    try:
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