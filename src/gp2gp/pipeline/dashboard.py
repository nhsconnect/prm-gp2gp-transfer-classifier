import sys
from datetime import datetime

from argparse import ArgumentParser

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc

from gp2gp.spine.sources import read_spine_csv_gz
from gp2gp.spine.transformers import (
    group_into_conversations,
    parse_conversation,
    filter_conversations_by_request_started_time,
)
from gp2gp.service.transformers import (
    derive_transfer,
    filter_failed_transfers,
    filter_pending_transfers,
    calculate_sla_by_practice,
    filter_practices,
)
from gp2gp.dashboard.transformers import construct_service_dashboard_data
from gp2gp.dashboard.sinks import write_service_dashboard_json


def _list_str(values):
    return values.split(",")


def parse_dashboard_pipeline_arguments(args):
    parser = ArgumentParser(description="GP2GP Service dashboard data pipeline")
    parser.add_argument("--month", type=int)
    parser.add_argument("--year", type=int)
    parser.add_argument("--ods-codes", type=_list_str)
    parser.add_argument("--spine-files", type=_list_str)
    return parser.parse_args(args)


def read_messages(filepaths):
    for filepath in filepaths:
        with open(filepath, "rb") as f:
            yield from read_spine_csv_gz(f)


def parse_conversations(conversations):
    for conversation in conversations:
        gp2gp_conversation = parse_conversation(conversation)
        if gp2gp_conversation is not None:
            yield gp2gp_conversation


def derive_transfers(conversations):
    for conversation in conversations:
        yield derive_transfer(conversation)


def process_messages(messages, start, end, practice_ods_codes):
    conversations = group_into_conversations(messages)
    parsed_conversations = parse_conversations(conversations)
    conversations_started_in_range = filter_conversations_by_request_started_time(
        parsed_conversations, start, end
    )
    transfers = derive_transfers(conversations_started_in_range)
    successful_transfers = filter_failed_transfers(transfers)
    completed_transfers = filter_pending_transfers(successful_transfers)
    sla_metrics = calculate_sla_by_practice(completed_transfers)
    sla_metrics_filtered = filter_practices(sla_metrics, practice_ods_codes)
    dashboard_data = construct_service_dashboard_data(
        sla_metrics_filtered, year=start.year, month=start.month
    )
    return dashboard_data


def write_service_dashboard_jsonfile(dashboard_data, month):
    month_name = month.strftime("%b").lower()
    year = month.strftime("%Y")
    output_filepath = f"dashboard_{month_name}_{year}.json"
    with open(output_filepath, "w") as f:
        write_service_dashboard_json(dashboard_data, f)


def main():
    args = parse_dashboard_pipeline_arguments(sys.argv[1:])

    metric_month = datetime(args.year, args.month, 1, tzinfo=tzutc())
    next_month = metric_month + relativedelta(months=1)

    spine_messages = read_messages(args.spine_files)
    service_dashboard_data = process_messages(
        spine_messages, metric_month, next_month, args.ods_codes
    )

    write_service_dashboard_jsonfile(service_dashboard_data, metric_month)
