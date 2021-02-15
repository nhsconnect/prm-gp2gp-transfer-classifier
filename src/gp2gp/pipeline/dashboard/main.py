import sys
from dataclasses import asdict
from datetime import datetime
from typing import List

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
from gp2gp.pipeline.dashboard.core import calculate_dashboard_data, parse_transfers_from_messages
from gp2gp.service.models import Transfer
from gp2gp.spine.sources import construct_messages_from_splunk_items
import pyarrow as pa
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


def _build_transfer_table(transfers: List[Transfer]):
    return pa.table(
        {
            "conversation_id": [t.conversation_id for t in transfers],
            "sla_duration": [
                int(t.sla_duration.total_seconds()) if t.sla_duration is not None else None
                for t in transfers
            ],
            "requesting_practice_asid": [t.requesting_practice_asid for t in transfers],
            "sending_practice_asid": [t.sending_practice_asid for t in transfers],
            "final_error_code": [t.final_error_code for t in transfers],
            "intermediate_error_codes": [t.intermediate_error_codes for t in transfers],
            "status": [t.status.value for t in transfers],
            "date_requested": [t.date_requested for t in transfers],
            "date_completed": [t.date_completed for t in transfers],
        },
        schema=pa.schema(
            [
                ("conversation_id", pa.string()),
                ("sla_duration", pa.int64()),
                ("requesting_practice_asid", pa.string()),
                ("sending_practice_asid", pa.string()),
                ("final_error_code", pa.int64()),
                ("intermediate_error_codes", pa.list_(pa.int64())),
                ("status", pa.string()),
                ("date_requested", pa.date64()),
                ("date_completed", pa.date64()),
            ]
        ),
    )


def _upload_transfers_parquet_object(transfers, s3_filesystem, s3_path):
    transfer_table = _build_transfer_table(transfers)
    write_table(transfer_table, where=s3_path, filesystem=s3_filesystem)


def _write_transfers_parquet_file(transfers, output_file_path):
    transfer_table = _build_transfer_table(transfers)
    write_table(transfer_table, output_file_path)


def _read_spine_csv_gz_files(file_paths):
    items = read_gzip_csv_files(file_paths)
    return construct_messages_from_splunk_items(items)


def _get_time_range(year, month):
    metric_month = datetime(year, month, 1, tzinfo=tzutc())
    next_month = metric_month + relativedelta(months=1)
    return DateTimeRange(metric_month, next_month)


def _is_outputting_to_file(args):
    return (
        args.organisation_metadata_output_file
        and args.practice_metrics_output_file
        and args.transfers_output_file
    )


def _is_outputting_to_s3(args):
    return (
        args.organisation_metadata_output_key
        and args.practice_metrics_output_key
        and args.transfers_output_key
    )


def main():
    args = parse_dashboard_pipeline_arguments(sys.argv[1:])
    time_range = _get_time_range(args.year, args.month)

    organisation_data = read_json_file(args.organisation_list_file)
    organisation_metadata = construct_organisation_list_from_dict(data=organisation_data)

    spine_messages = _read_spine_csv_gz_files(args.input_files)
    transfers = list(parse_transfers_from_messages(spine_messages, time_range))
    service_dashboard_data = calculate_dashboard_data(
        transfers, organisation_metadata.practices, time_range
    )

    service_dashboard_metadata = construct_service_dashboard_metadata(organisation_metadata)

    if _is_outputting_to_file(args):
        _write_dashboard_json_file(
            service_dashboard_metadata, args.organisation_metadata_output_file
        )
        _write_dashboard_json_file(service_dashboard_data, args.practice_metrics_output_file)
        _write_transfers_parquet_file(transfers, args.transfers_output_file)
    elif _is_outputting_to_s3(args):
        s3 = boto3.resource("s3", endpoint_url=args.s3_endpoint_url)
        s3_filesystem = S3FileSystem(endpoint_override=args.s3_endpoint_url)

        bucket_name = args.output_bucket

        _upload_dashboard_json_object(
            service_dashboard_metadata,
            s3.Object(bucket_name, args.organisation_metadata_output_key),
        )
        _upload_dashboard_json_object(
            service_dashboard_data, s3.Object(bucket_name, args.practice_metrics_output_key)
        )
        _upload_transfers_parquet_object(
            transfers, s3_filesystem, bucket_name + "/" + args.transfers_output_key
        )
