import json
import sys
from datetime import datetime

from argparse import ArgumentParser

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc

from gp2gp.odsportal.models import PracticeDetails
from gp2gp.spine.sources import read_spine_csv_gz_files
from gp2gp.spine.transformers import (
    group_into_conversations,
    parse_conversation,
    filter_conversations_by_request_started_time,
)
from gp2gp.service.transformers import (
    derive_transfers,
    filter_failed_transfers,
    filter_pending_transfers,
    calculate_sla_by_practice,
)
from gp2gp.dashboard.transformers import construct_service_dashboard_data
from gp2gp.io.json import write_as_json


def _list_str(values):
    return values.split(",")


def parse_dashboard_pipeline_arguments(args):
    parser = ArgumentParser(description="GP2GP Service dashboard data pipeline")
    parser.add_argument("--month", type=int)
    parser.add_argument("--year", type=int)
    parser.add_argument("--practice-list-file", type=str)
    parser.add_argument("--input-files", type=_list_str)
    parser.add_argument("--output-file", type=str)
    return parser.parse_args(args)


def parse_conversations(conversations):
    for conversation in conversations:
        gp2gp_conversation = parse_conversation(conversation)
        if gp2gp_conversation is not None:
            yield gp2gp_conversation


def process_messages(messages, start, end, practice_list):
    conversations = group_into_conversations(messages)
    parsed_conversations = parse_conversations(conversations)
    conversations_started_in_range = filter_conversations_by_request_started_time(
        parsed_conversations, start, end
    )
    transfers = derive_transfers(conversations_started_in_range)
    successful_transfers = filter_failed_transfers(transfers)
    completed_transfers = filter_pending_transfers(successful_transfers)
    sla_metrics = calculate_sla_by_practice(practice_list, completed_transfers)
    dashboard_data = construct_service_dashboard_data(
        sla_metrics, year=start.year, month=start.month
    )
    return dashboard_data


def write_service_dashboard_json_file(dashboard_data, output_file_path):
    with open(output_file_path, "w") as f:
        write_as_json(dashboard_data, f)


def main():
    args = parse_dashboard_pipeline_arguments(sys.argv[1:])

    metric_month = datetime(args.year, args.month, 1, tzinfo=tzutc())
    next_month = metric_month + relativedelta(months=1)

    output_file_path = args.output_file

    with open(args.practice_list_file) as f:
        data = f.read()
        practice_list = [
            PracticeDetails(ods_code=p["odsCode"], name=p["name"])
            for p in json.loads(data)["practices"]
        ]

    spine_messages = read_spine_csv_gz_files(args.input_files)
    service_dashboard_data = process_messages(
        spine_messages, metric_month, next_month, practice_list
    )

    write_service_dashboard_json_file(service_dashboard_data, output_file_path)
