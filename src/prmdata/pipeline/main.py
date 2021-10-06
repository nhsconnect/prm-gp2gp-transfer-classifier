import logging

import boto3
from os import environ

from prmdata.domain.gp2gp.transfer_service import TransferObservabilityProbe, module_logger
from prmdata.pipeline.io import TransferClassifierIO, TransferClassifierS3UriResolver
from prmdata.utils.io.json_formatter import JsonFormatter
from prmdata.domain.datetime import MonthlyReportingWindow
from prmdata.utils.io.s3 import S3DataManager
from prmdata.pipeline.parse_transfers_from_messages import (
    parse_transfers_from_messages,
)
from prmdata.pipeline.config import DataPipelineConfig

logger = logging.getLogger("prmdata")


def _setup_logger():
    logger.setLevel(logging.INFO)
    formatter = JsonFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    _setup_logger()

    config = DataPipelineConfig.from_environment_variables(environ)

    s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
    s3_manager = S3DataManager(s3)

    reporting_window = MonthlyReportingWindow.prior_to(config.date_anchor)

    uris = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=config.input_spine_data_bucket,
        transfers_bucket=config.output_transfer_data_bucket,
    )

    io = TransferClassifierIO(s3_manager)

    input_path = uris.spine_messages(reporting_window)
    spine_messages = io.read_spine_messages(input_path)

    conversation_cutoff = config.conversation_cutoff

    transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)
    transfers = parse_transfers_from_messages(
        spine_messages=spine_messages,
        reporting_window=reporting_window,
        conversation_cutoff=conversation_cutoff,
        observability_probe=transfer_observability_probe,
    )

    output_metadata = {
        "date-anchor": config.date_anchor.isoformat(),
        "cutoff-days": str(config.conversation_cutoff.days),
        "build-tag": config.build_tag,
    }

    output_path = uris.gp2gp_transfers(reporting_window)

    io.write_transfers(transfers, output_path, output_metadata)


if __name__ == "__main__":
    main()
