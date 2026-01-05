"""Custom logger setup."""

import atexit
import logging.config
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure logging from YAML config file and start queue handler."""
    config_file = Path(__file__).parent / "config.yaml"
    config = yaml.safe_load(config_file.read_text())

    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None and hasattr(queue_handler, "listener"):
        queue_handler.listener.start()  # type: ignore[attr-defined]
        atexit.register(queue_handler.listener.stop)  # type: ignore[attr-defined]


def main() -> None:
    """Run logging test with various log levels."""
    setup_logging()

    logger.debug("debug message", extra={"x": "hello"})
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")

    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception("exception message")


if __name__ == "__main__":
    main()
