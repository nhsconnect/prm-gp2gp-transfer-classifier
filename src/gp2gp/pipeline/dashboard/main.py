import sys
from dataclasses import asdict
from datetime import datetime

import boto3
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from gp2gp.dashboard.transformers import construct_service_dashboard_metadata
from gp2gp.date.range import DateTimeRange

from gp2gp.io.csv import read_gzip_csv_files
from gp2gp.io.dictionary import camelize_dict
from gp2gp.io.json import write_json_file, read_json_file, upload_json_object
from gp2gp.odsportal.sources import construct_organisation_list_from_dict
from gp2gp.pipeline.dashboard.args import parse_dashboard_pipeline_arguments
from gp2gp.pipeline.dashboard.core import calculate_dashboard_data
from gp2gp.spine.sources import construct_messages_from_splunk_items


def write_dashboard_json_file(dashboard_data, output_file_path):
    content_dict = asdict(dashboard_data)
    camelized_dict = camelize_dict(content_dict)
    write_json_file(camelized_dict, output_file_path)


def upload_dashboard_json_object(dashboard_data, s3_object):
    content_dict = asdict(dashboard_data)
    camelized_dict = camelize_dict(content_dict)
    upload_json_object(camelized_dict, s3_object)


def read_spine_csv_gz_files(file_paths):
    items = read_gzip_csv_files(file_paths)
    return construct_messages_from_splunk_items(items)


def main():
    args = parse_dashboard_pipeline_arguments(sys.argv[1:])

    metric_month = datetime(args.year, args.month, 1, tzinfo=tzutc())
    next_month = metric_month + relativedelta(months=1)
    time_range = DateTimeRange(metric_month, next_month)

    organisation_data = read_json_file(args.organisation_list_file)
    organisation_metadata = construct_organisation_list_from_dict(data=organisation_data)

    spine_messages = read_spine_csv_gz_files(args.input_files)
    service_dashboard_data = calculate_dashboard_data(
        spine_messages, organisation_metadata.practices, time_range
    )

    service_dashboard_metadata = construct_service_dashboard_metadata(organisation_metadata)

    if args.organisation_metadata_output_file is not None:
        write_dashboard_json_file(
            service_dashboard_metadata, args.organisation_metadata_output_file
        )

    if args.practice_metrics_output_file is not None:
        write_dashboard_json_file(service_dashboard_data, args.practice_metrics_output_file)

    s3 = boto3.resource("s3", endpoint_url=args.s3_endpoint_url)
    bucket_name = args.output_bucket
    if args.organisation_metadata_output_key is not None:
        upload_dashboard_json_object(
            service_dashboard_metadata,
            s3.Object(bucket_name, args.organisation_metadata_output_key),
        )
    if args.practice_metrics_output_key is not None:
        upload_dashboard_json_object(
            service_dashboard_data, s3.Object(bucket_name, args.practice_metrics_output_key)
        )
