import sys
from argparse import ArgumentParser
from dataclasses import asdict

from prmdata.utils.io.csv import read_gzip_csv_file
from prmdata.utils.io.json import write_json_file
from prmdata.domain.ods_portal.sources import (
    OdsDataFetcher,
    construct_organisation_metadata_from_ods_portal_response,
    ODS_PORTAL_SEARCH_URL,
    PRACTICE_SEARCH_PARAMS,
    CCG_SEARCH_PARAMS,
    construct_asid_to_ods_mappings,
)


def parse_ods_portal_pipeline_arguments(args):
    parser = ArgumentParser(description="ODS portal pipeline")
    parser.add_argument("--output-file", type=str, required=True)
    parser.add_argument("--mapping-file", type=str, required=True)
    parser.add_argument("--search-url", type=str, required=False, default=ODS_PORTAL_SEARCH_URL)
    return parser.parse_args(args)


def main():
    args = parse_ods_portal_pipeline_arguments(sys.argv[1:])

    data_fetcher = OdsDataFetcher(search_url=args.search_url)

    practice_data = data_fetcher.fetch_organisation_data(PRACTICE_SEARCH_PARAMS)
    ccg_data = data_fetcher.fetch_organisation_data(CCG_SEARCH_PARAMS)

    asid_mapping = construct_asid_to_ods_mappings(read_gzip_csv_file(args.mapping_file))

    organisation_metadata = construct_organisation_metadata_from_ods_portal_response(
        practice_data, ccg_data, asid_mapping
    )

    write_json_file(asdict(organisation_metadata), args.output_file)
