import sys
from argparse import ArgumentParser
from dataclasses import asdict

from gp2gp.io.json import write_json_file
from gp2gp.odsportal.sources import (
    OdsPracticeDataFetcher,
    construct_practice_list_from_ods_portal_response,
)


def parse_ods_portal_pipeline_arguments(args):
    parser = ArgumentParser(description="ODS portal pipeline")
    parser.add_argument("--output-file", type=str)
    return parser.parse_args(args)


def main():
    args = parse_ods_portal_pipeline_arguments(sys.argv[1:])

    output_file_path = args.output_file

    data_fetcher = OdsPracticeDataFetcher()
    data = data_fetcher.fetch_practice_data()

    practice_list = construct_practice_list_from_ods_portal_response(data)

    write_json_file(asdict(practice_list), output_file_path)
