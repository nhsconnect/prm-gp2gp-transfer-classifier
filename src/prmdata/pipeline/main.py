import logging
import sys
from os import environ

from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.mi_runner import MiRunner
from prmdata.pipeline.spine_runner import SpineRunner
from prmdata.utils.input_output.json_formatter import JsonFormatter

logger = logging.getLogger("prmdata")


def _setup_logger():
    logger.setLevel(logging.INFO)
    formatter = JsonFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    config = None
    try:
        config = TransferClassifierConfig.from_environment_variables(environ)
        _setup_logger()
        logger.info(
            "Running transfer classifier",
            extra={"event": "RUNNING_TRANSFER_CLASSIFIER", "config": config.__str__()},
        )

        if config.classify_mi_events is not False:
            return MiRunner(config).run()
        else:
            SpineRunner(config).run()
    except Exception as ex:
        logger.error(str(ex), extra={"event": "FAILED_TO_RUN_MAIN", "config": config.__str__()})
        sys.exit("Failed to run main, exiting...")


if __name__ == "__main__":
    main()
