import sys
from dataclasses import asdict
from datetime import datetime

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from gp2gp.date.range import DateTimeRange

from gp2gp.io.csv import read_gzip_csv_files
from gp2gp.io.dictionary import camelize_dict
from gp2gp.io.json import write_json_file, read_json_file
from gp2gp.odsportal.sources import construct_practice_list_from_dict
from gp2gp.pipeline.dashboard.args import parse_dashboard_pipeline_arguments
from gp2gp.pipeline.dashboard.core import calculate_dashboard_data
from gp2gp.spine.sources import construct_messages_from_splunk_items


def write_service_dashboard_json_file(dashboard_data, output_file_path):
    content_dict = asdict(dashboard_data)
    camelized_dict = camelize_dict(content_dict)
    write_json_file(camelized_dict, output_file_path)


def read_spine_csv_gz_files(file_paths):
    items = read_gzip_csv_files(file_paths)
    return construct_messages_from_splunk_items(items)


def main():
    args = parse_dashboard_pipeline_arguments(sys.argv[1:])

    metric_month = datetime(args.year, args.month, 1, tzinfo=tzutc())
    next_month = metric_month + relativedelta(months=1)
    time_range = DateTimeRange(metric_month, next_month)

    practice_metrics_output_file_path = args.practice_metrics_output_file
    practice_metadata_output_file_path = args.practice_metadata_output_file

    practice_data = read_json_file(args.practice_list_file)
    practice_metadata = construct_practice_list_from_dict(practice_data)

    write_service_dashboard_json_file(practice_metadata, practice_metadata_output_file_path)

    spine_messages = read_spine_csv_gz_files(args.input_files)
    service_dashboard_data = calculate_dashboard_data(
        spine_messages, practice_metadata.practices, time_range
    )

    write_service_dashboard_json_file(service_dashboard_data, practice_metrics_output_file_path)
