from gp2gp.pipeline.odsportal import parse_ods_portal_pipeline_arguments

from argparse import Namespace


def test_parse_ods_portal_arguments():
    args = [
        "--output-file",
        "data/practice-list.json",
    ]

    expected = Namespace(output_file="data/practice-list.json")

    actual = parse_ods_portal_pipeline_arguments(args)

    assert actual == expected
