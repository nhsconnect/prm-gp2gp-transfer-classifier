from dataclasses import asdict
from datetime import datetime
import logging

import boto3
from botocore.exceptions import ClientError
from os import environ
import json

from itertools import chain
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from prmdata.domain.data_platform.organisation_metadata import construct_organisation_metadata
from prmdata.utils.date.range import DateTimeRange
from prmdata.utils.io.json import upload_json_object
from urllib.parse import urlparse
from prmdata.utils.io.csv import read_gzip_csv_file
from prmdata.utils.io.dictionary import camelize_dict
from prmdata.domain.ods_portal.models import construct_organisation_list_from_dict
from prmdata.pipeline.platform_metrics_calculator.core import (
    calculate_practice_metrics_data,
    parse_transfers_from_messages,
    calculate_national_metrics_data,
)
from prmdata.pipeline.platform_metrics_calculator.config import DataPipelineConfig
from prmdata.domain.gp2gp.transfer import convert_transfers_to_table
from prmdata.domain.spine.message import construct_messages_from_splunk_items
from pyarrow.parquet import write_table
from pyarrow.fs import S3FileSystem

logger = logging.getLogger(__name__)

VERSION = "v2"


def _upload_data_platform_json_object(platform_data, s3_object):
    content_dict = asdict(platform_data)
    camelized_dict = camelize_dict(content_dict)
    upload_json_object(camelized_dict, s3_object)


def _read_spine_transfer_csv_gz_files(input_transfer_data, input_transfer_overflow_data):
    input_transfer_data_items = construct_messages_from_splunk_items(
        read_gzip_csv_file(input_transfer_data)
    )
    input_overflow_overflow_data_items = construct_messages_from_splunk_items(
        read_gzip_csv_file(input_transfer_overflow_data)
    )
    return chain(input_transfer_data_items, input_overflow_overflow_data_items)


def _get_time_range(year, month):
    metric_month = datetime(year, month, 1, tzinfo=tzutc())
    next_month = metric_month + relativedelta(months=1)
    return DateTimeRange(metric_month, next_month)


def generate_s3_path(year: int, month: int, bucket_name: str, file_name: str) -> str:
    return f"s3://{bucket_name}/{VERSION}/{year}/{month}/{file_name}"


def _create_s3_object(s3, url_string):
    object_url = urlparse(url_string)
    s3_bucket = object_url.netloc
    s3_key = object_url.path.lstrip("/")
    return s3.Object(s3_bucket, s3_key)


def generate_s3_path_for_input_transfer_data(year, month, input_bucket):
    month_dict = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    a_month = relativedelta(months=1)
    overflow_date = datetime(year, month, 1) + a_month
    overflow_path = (
        f"s3://{input_bucket}/{VERSION}/{overflow_date.year}/{overflow_date.month}"
        f"/overflow/{month_dict[overflow_date.month]}-{overflow_date.year}.csv.gz"
    )

    input_path = f"s3://{input_bucket}/{VERSION}/{year}/{month}/{month_dict[month]}-{year}.csv.gz"
    return input_path, overflow_path


def main():
    config = DataPipelineConfig.from_environment_variables(environ)

    logging.basicConfig(level=logging.INFO)

    s3 = boto3.resource("s3", endpoint_url=config.s3_endpoint_url)
    organisation_list_path = generate_s3_path(
        config.year, config.month, config.organisation_list_bucket, "organisationMetadata.json"
    )
    organisation_list_object = _create_s3_object(s3, organisation_list_path)
    try:
        organisation_list_body = json.loads(
            organisation_list_object.get()["Body"].read().decode("utf-8")
        )
    except ClientError as error:
        raise error
    organisation_metadata = construct_organisation_list_from_dict(data=organisation_list_body)

    (
        input_transfer_data_s3_path,
        input_transfer_overflow_data_s3_path,
    ) = generate_s3_path_for_input_transfer_data(config.year, config.month, config.input_bucket)
    input_transfer_data = _create_s3_object(s3, input_transfer_data_s3_path).get()["Body"]
    input_transfer_overflow_data = _create_s3_object(
        s3, input_transfer_overflow_data_s3_path
    ).get()["Body"]

    spine_messages = _read_spine_transfer_csv_gz_files(
        input_transfer_data, input_transfer_overflow_data
    )
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

    practice_metrics_file_name = "practiceMetrics.json"
    organisation_metadata_file_name = "organisationMetadata.json"
    national_metrics_file_name = "nationalMetrics.json"
    transfers_file_name = "transfers.parquet"

    bucket_name = config.output_bucket
    s3_path = f"{VERSION}/{config.year}/{config.month}"

    _upload_data_platform_json_object(
        practice_metrics_data,
        s3.Object(bucket_name, f"{s3_path}/{practice_metrics_file_name}"),
    )
    _upload_data_platform_json_object(
        organisation_metadata,
        s3.Object(bucket_name, f"{s3_path}/{organisation_metadata_file_name}"),
    )
    _upload_data_platform_json_object(
        national_metrics_data,
        s3.Object(bucket_name, f"{s3_path}/{national_metrics_file_name}"),
    )
    write_table(
        table=transfer_table,
        where=bucket_name + "/" + f"{s3_path}/{transfers_file_name}",
        filesystem=S3FileSystem(endpoint_override=config.s3_endpoint_url),
    )


if __name__ == "__main__":
    main()
