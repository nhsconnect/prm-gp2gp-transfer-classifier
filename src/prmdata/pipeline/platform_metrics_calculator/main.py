from dataclasses import asdict
from datetime import datetime
import logging

import boto3
from os import environ

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from prmdata.domain.data_platform.organisation_metadata import construct_organisation_metadata
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.date.range import DateTimeRange
from prmdata.utils.date_anchor import DateAnchor
from prmdata.utils.io.s3 import S3DataManager
from prmdata.utils.io.dictionary import camelize_dict
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

VERSION = "v2"


def _create_platform_json_object(platform_data):
    content_dict = asdict(platform_data)
    return camelize_dict(content_dict)


def _get_time_range(year, month):
    metric_month = datetime(year, month, 1, tzinfo=tzutc())
    previous_month = metric_month - relativedelta(months=1)
    return DateTimeRange(previous_month, metric_month)


def main():
    config = DataPipelineConfig.from_environment_variables(environ)

    logging.basicConfig(level=logging.INFO)

    s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
    s3_manager = S3DataManager(s3)

    date_anchor = DateAnchor(datetime(year=config.year, month=config.month, day=1))
    metrics_io = PlatformMetricsIO(
        date_anchor=date_anchor,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=config.organisation_metadata_bucket,
        gp2gp_spine_bucket=config.output_transfer_data_bucket,
    )

    organisation_metadata = metrics_io.read_ods_metadata()

    spine_messages = metrics_io.read_spine_messages()

    time_range = _get_time_range(config.year, config.month)
    transfers = list(parse_transfers_from_messages(spine_messages, time_range))
    practice_metrics_data = calculate_practice_metrics_data(
        transfers, organisation_metadata.practices, time_range
    )
    national_metrics_data = calculate_national_metrics_data(
        transfers=transfers, time_range=time_range
    )
    organisation_metadata = construct_organisation_metadata(organisation_metadata)
    transfer_table = convert_transfers_to_table(transfers)

    bucket_name = config.output_transfer_data_bucket

    previous_month = date_anchor.previous_month
    s3_path = f"{bucket_name}/{VERSION}/{previous_month.year}/{previous_month.month}"

    s3_manager.write_json(
        object_uri=f"s3://{s3_path}/practiceMetrics.json",
        data=_create_platform_json_object(practice_metrics_data),
    )
    s3_manager.write_json(
        object_uri=f"s3://{s3_path}/organisationMetadata.json",
        data=_create_platform_json_object(organisation_metadata),
    )
    s3_manager.write_json(
        object_uri=f"s3://{s3_path}/nationalMetrics.json",
        data=_create_platform_json_object(national_metrics_data),
    )
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
