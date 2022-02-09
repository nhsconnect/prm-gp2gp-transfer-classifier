import logging
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
    _setup_logger()
    config = TransferClassifierConfig.from_environment_variables(environ)
    TransferClassifier(config).run()


if __name__ == "__main__":
    main()
