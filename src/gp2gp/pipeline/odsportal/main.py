import sys
from argparse import ArgumentParser
from dataclasses import asdict

from gp2gp.io.json import write_json_file
from gp2gp.odsportal.sources import (
    OdsPracticeDataFetcher,
    construct_practice_metadata_from_ods_portal_response,
    ODS_PORTAL_SEARCH_URL,
)


def parse_ods_portal_pipeline_arguments(args):
    parser = ArgumentParser(description="ODS portal pipeline")
    parser.add_argument("--output-file", type=str, required=True)
    parser.add_argument("--search-url", type=str, required=False, default=ODS_PORTAL_SEARCH_URL)
    return parser.parse_args(args)


def main():
    args = parse_ods_portal_pipeline_arguments(sys.argv[1:])

    data_fetcher = OdsPracticeDataFetcher(search_url=args.search_url)

    data = data_fetcher.fetch_practice_data()

    practice_metadata = construct_practice_metadata_from_ods_portal_response(data)

    write_json_file(asdict(practice_metadata), args.output_file)
