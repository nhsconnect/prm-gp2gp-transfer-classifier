import logging
import sys
from os import environ

from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.transfer_classifier import TransferClassifier
from prmdata.utils.input_output.json_formatter import JsonFormatter

logger = logging.getLogger("prmdata")


def _setup_logger():
    logger.setLevel(logging.INFO)
    formatter = JsonFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    config = {}
    try:
        _setup_logger()
        config = TransferClassifierConfig.from_environment_variables(environ)
        TransferClassifier(config).run()
    except Exception as ex:
        logger.error("Failed to run main", extra={"event": "FAILED_TO_RUN_MAIN", "config": config})
        logger.error("Failed to run main with config: " + str(config))
        logger.error(str(ex), extra={"event": "FAILED_TO_RUN_MAIN", "config": config})
        logger.info("Exiting main")
        logging.info("Logging with logging")
        logging.info("Logging with logging", config)
        sys.exit("Failed to run main")


if __name__ == "__main__":
    main()
