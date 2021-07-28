import logging

import boto3
from os import environ

from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from prmdata.utils.io.s3 import S3DataManager
from prmdata.pipeline.platform_metrics_calculator.core import (
    calculate_practice_metrics_data,
    parse_transfers_from_messages,
    calculate_national_metrics_data,
)
from prmdata.pipeline.platform_metrics_calculator.config import DataPipelineConfig
from prmdata.domain.gp2gp.transfer import convert_transfers_to_table
from pyarrow.parquet import write_table
from pyarrow.fs import S3FileSystem

logger = logging.getLogger(__name__)

_PARQUET_VERSION = "v4"


def main():
    config = DataPipelineConfig.from_environment_variables(environ)

    logging.basicConfig(level=logging.INFO)

    s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
    s3_manager = S3DataManager(s3)

    reporting_window = MonthlyReportingWindow.prior_to(config.date_anchor)
    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=config.organisation_metadata_bucket,
        gp2gp_spine_bucket=config.input_transfer_data_bucket,
        dashboard_data_bucket=config.output_transfer_data_bucket,
    )

    organisation_metadata = metrics_io.read_ods_metadata()

    spine_messages = metrics_io.read_spine_messages()

    conversation_cutoff = config.conversation_cutoff

    transfers = list(
        parse_transfers_from_messages(spine_messages, reporting_window, conversation_cutoff)
    )
    practice_metrics_data = calculate_practice_metrics_data(
        transfers, organisation_metadata, reporting_window
    )
    national_metrics_data = calculate_national_metrics_data(
        transfers=transfers, reporting_window=reporting_window
    )
    transfer_table = convert_transfers_to_table(transfers)

    bucket_name = config.output_transfer_data_bucket

    s3_path = (
        f"{bucket_name}/"
        f"{_PARQUET_VERSION}/"
        f"{reporting_window.metric_year}/"
        f"{reporting_window.metric_month}"
    )

    metrics_io.write_practice_metrics(practice_metrics_data)
    metrics_io.write_national_metrics(national_metrics_data)

    write_table(
        table=transfer_table,
        where=f"{s3_path}/transfers.parquet",
        filesystem=S3FileSystem(endpoint_override=config.s3_endpoint_url),
    )
    logger.info(
        f"Successfully calculated platform metrics and uploaded to s3://{bucket_name}/{s3_path}"
    )


if __name__ == "__main__":
    main()
