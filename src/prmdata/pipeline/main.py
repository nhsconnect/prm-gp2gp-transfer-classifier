import logging

import boto3
from os import environ

from prmdata.domain.gp2gp.transfer_service import TransferObservabilityProbe, module_logger
from prmdata.pipeline.io import TransferClassifierIO
from prmdata.utils.io.json_formatter import JsonFormatter
from prmdata.domain.datetime import MonthlyReportingWindow
from prmdata.utils.io.s3 import S3DataManager
from prmdata.pipeline.parse_transfers_from_messages import (
    parse_transfers_from_messages,
)
from prmdata.pipeline.config import DataPipelineConfig
from prmdata.pipeline.arrow import convert_transfers_to_table

logger = logging.getLogger("prmdata")

_PARQUET_VERSION = "v5"


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
    transfer_classifier_io = TransferClassifierIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        gp2gp_spine_bucket=config.input_spine_data_bucket,
    )

    spine_messages = transfer_classifier_io.read_spine_messages()

    conversation_cutoff = config.conversation_cutoff

    transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)
    transfers = parse_transfers_from_messages(
        spine_messages=spine_messages,
        reporting_window=reporting_window,
        conversation_cutoff=conversation_cutoff,
        observability_probe=transfer_observability_probe,
    )

    transfer_table = convert_transfers_to_table(transfers)

    s3_path = (
        f"{config.output_transfer_data_bucket}/"
        f"{_PARQUET_VERSION}/"
        f"{reporting_window.metric_year}/"
        f"{reporting_window.metric_month}"
    )
    s3_file_name = (
        f"{reporting_window.metric_year}-{reporting_window.metric_month}-transfers.parquet"
    )

    output_metadata = {
        "date-anchor": config.date_anchor.isoformat(),
        "cutoff-days": str(config.conversation_cutoff.days),
        "build-tag": config.build_tag,
    }

    s3_manager.write_parquet(transfer_table, f"s3://{s3_path}/{s3_file_name}", output_metadata)


if __name__ == "__main__":
    main()
