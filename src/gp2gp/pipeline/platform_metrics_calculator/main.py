import sys
from dataclasses import asdict
from datetime import datetime

import boto3
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from gp2gp.domain.data_platform.organisation_metadata import construct_organisation_metadata
from gp2gp.utils.date.range import DateTimeRange

from gp2gp.utils.io.csv import read_gzip_csv_files
from gp2gp.utils.io.dictionary import camelize_dict
from gp2gp.utils.io.json import write_json_file, read_json_file, upload_json_object
from gp2gp.domain.ods_portal.sources import construct_organisation_list_from_dict
from gp2gp.pipeline.platform_metrics_calculator.args import parse_dashboard_pipeline_arguments
from gp2gp.pipeline.platform_metrics_calculator.core import (
    calculate_practice_metrics_data,
    parse_transfers_from_messages,
    calculate_national_metrics_data,
)
from gp2gp.domain.service.transfer import convert_transfers_to_table
from gp2gp.domain.spine.sources import construct_messages_from_splunk_items
from pyarrow.parquet import write_table
from pyarrow.fs import S3FileSystem


def _write_dashboard_json_file(dashboard_data, output_file_path):
    content_dict = asdict(dashboard_data)
    camelized_dict = camelize_dict(content_dict)
    write_json_file(camelized_dict, output_file_path)


def _upload_dashboard_json_object(dashboard_data, s3_object):
    content_dict = asdict(dashboard_data)
    camelized_dict = camelize_dict(content_dict)
    upload_json_object(camelized_dict, s3_object)


def _read_spine_csv_gz_files(file_paths):
    items = read_gzip_csv_files(file_paths)
    return construct_messages_from_splunk_items(items)


def _get_time_range(year, month):
    metric_month = datetime(year, month, 1, tzinfo=tzutc())
    next_month = metric_month + relativedelta(months=1)
    return DateTimeRange(metric_month, next_month)


def _is_outputting_to_file(args):
    return args.output_directory


def _is_outputting_to_s3(args):
    return args.output_bucket


def main():
    args = parse_dashboard_pipeline_arguments(sys.argv[1:])
    time_range = _get_time_range(args.year, args.month)

    organisation_data = read_json_file(args.organisation_list_file)
    organisation_metadata = construct_organisation_list_from_dict(data=organisation_data)

    spine_messages = _read_spine_csv_gz_files(args.input_files)
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

    if _is_outputting_to_file(args):
        _write_dashboard_json_file(
            practice_metrics_data,
            f"{args.output_directory}/{args.month}-{args.year}-{practice_metrics_file_name}",
        )
        _write_dashboard_json_file(
            organisation_metadata,
            f"{args.output_directory}/{args.month}-{args.year}-{organisation_metadata_file_name}",
        )
        _write_dashboard_json_file(
            national_metrics_data,
            f"{args.output_directory}/{args.month}-{args.year}-{national_metrics_file_name}",
        )
        write_table(
            transfer_table,
            f"{args.output_directory}/{args.month}-{args.year}-{transfers_file_name}",
        )
    elif _is_outputting_to_s3(args):
        s3 = boto3.resource("s3", endpoint_url=args.s3_endpoint_url)

        bucket_name = args.output_bucket
        version = "v2"
        s3_path = f"{version}/{args.year}/{args.month}"

        _upload_dashboard_json_object(
            practice_metrics_data,
            s3.Object(bucket_name, f"{s3_path}/{practice_metrics_file_name}"),
        )
        _upload_dashboard_json_object(
            organisation_metadata,
            s3.Object(bucket_name, f"{s3_path}/{organisation_metadata_file_name}"),
        )
        _upload_dashboard_json_object(
            national_metrics_data,
            s3.Object(bucket_name, f"{s3_path}/{national_metrics_file_name}"),
        )
        write_table(
            table=transfer_table,
            where=bucket_name + "/" + f"{s3_path}/{transfers_file_name}",
            filesystem=S3FileSystem(endpoint_override=args.s3_endpoint_url),
        )
