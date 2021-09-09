import logging

import boto3
from os import environ

from prmdata.domain.gp2gp.transfer_service import TransferObservabilityProbe, module_logger
from prmdata.pipeline.io import PlatformMetricsIO
from prmdata.utils.io.json_formatter import JsonFormatter
from prmdata.utils.reporting_window import MonthlyReportingWindow
from prmdata.utils.io.s3 import S3DataManager
from prmdata.pipeline.core import (
    parse_transfers_from_messages,
)
from prmdata.pipeline.config import DataPipelineConfig
from prmdata.pipeline.arrow import convert_transfers_to_table
from pyarrow.parquet import write_table
from pyarrow.fs import S3FileSystem

logger = logging.getLogger("prmdata")

_PARQUET_VERSION = "v4"


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
    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        gp2gp_spine_bucket=config.input_spine_data_bucket,
    )

    spine_messages = metrics_io.read_spine_messages()

    conversation_cutoff = config.conversation_cutoff

    transfer_observability_probe = TransferObservabilityProbe(logger=module_logger)
    transfers = parse_transfers_from_messages(
        spine_messages=spine_messages,
        reporting_window=reporting_window,
        conversation_cutoff=conversation_cutoff,
        observability_probe=transfer_observability_probe,
    )

    transfer_table = convert_transfers_to_table(transfers)

    transfer_data_bucket = config.output_transfer_data_bucket
    s3_path = (
        f"{config.output_transfer_data_bucket}/"
        f"{_PARQUET_VERSION}/"
        f"{reporting_window.metric_year}/"
        f"{reporting_window.metric_month}"
    )

    transfer_object_uri = f"s3://{transfer_data_bucket}/{s3_path}"

    logger.info(
        f"Attempting to upload: {transfer_object_uri}",
        extra={
            "event": "ATTEMPTING_UPLOAD_TRANSFER_PARQUET_TO_S3",
            "object_uri": transfer_object_uri,
        },
    )

    write_table(
        table=transfer_table,
        where=f"{s3_path}/transfers.parquet",
        filesystem=S3FileSystem(endpoint_override=config.s3_endpoint_url),
    )

    logger.info(
        f"Successfully uploaded to: {transfer_object_uri}",
        extra={"event": "UPLOADED_TRANSFER_PARQUET_TO_S3", "object_uri": transfer_object_uri},
    )


if __name__ == "__main__":
    main()
