from gp2gp.pipeline.dashboard import parse_dashboard_pipeline_arguments

from argparse import Namespace


def test_parse_dashboard_arguments():
    args = [
        "--month",
        "6",
        "--year",
        "2019",
        "--ods-codes",
        "a12345,b56789",
        "--spine-files",
        "data/jun.csv,data/july.csv",
    ]

    expected = Namespace(
        month=6,
        year=2019,
        ods_codes=["a12345", "b56789"],
        spine_files=["data/jun.csv", "data/july.csv"],
    )

    actual = parse_dashboard_pipeline_arguments(args)

    assert actual == expected
