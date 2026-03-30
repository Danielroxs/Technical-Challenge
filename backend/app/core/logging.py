import logging


def setup_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)

    # Reuse the existing logger to avoid adding duplicated handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Keep logs simple and readable for local development and debugging
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger