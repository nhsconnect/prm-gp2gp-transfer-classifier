from gp2gp.pipeline.odsportal import parse_ods_portal_pipeline_arguments

from argparse import Namespace


def test_parse_ods_portal_arguments():
    args = [
        "--output-file",
        "data/practice-list.json",
    ]

    expected = Namespace(output_file="data/practice-list.json", search_url=None)

    actual = parse_ods_portal_pipeline_arguments(args)

    assert actual == expected


def test_parse_optional_ods_portal_arguments():
    args = [
        "--output-file",
        "data/practice-list.json",
        "--search-url",
        "http://test.com",
    ]

    expected = Namespace(output_file="data/practice-list.json", search_url="http://test.com")

    actual = parse_ods_portal_pipeline_arguments(args)

    assert actual == expected
