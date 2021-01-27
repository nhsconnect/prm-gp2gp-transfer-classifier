from gp2gp.pipeline.odsportal.main import parse_ods_portal_pipeline_arguments

from argparse import Namespace


def test_parse_ods_portal_arguments():
    args = [
        "--output-file",
        "data/practice-list.json",
        "--mapping-file",
        "data/asid-mapping.csv",
    ]

    expected = Namespace(
        output_file="data/practice-list.json",
        mapping_file="data/asid-mapping.csv",
        search_url="https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations",
    )

    actual = parse_ods_portal_pipeline_arguments(args)

    assert actual == expected


def test_parse_optional_ods_portal_arguments():
    args = [
        "--output-file",
        "data/practice-list.json",
        "--mapping-file",
        "data/asid-mapping.csv",
        "--search-url",
        "http://test.com",
    ]

    expected = Namespace(
        output_file="data/practice-list.json",
        mapping_file="data/asid-mapping.csv",
        search_url="http://test.com",
    )

    actual = parse_ods_portal_pipeline_arguments(args)

    assert actual == expected
