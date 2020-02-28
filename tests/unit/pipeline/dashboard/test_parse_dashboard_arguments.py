from gp2gp.pipeline.dashboard import parse_dashboard_pipeline_arguments

from argparse import Namespace


def test_parse_dashboard_arguments():
    args = [
        "--month",
        "6",
        "--year",
        "2019",
        "--practice-list-file",
        "data/practice-list.json",
        "--input-files",
        "data/jun.csv,data/july.csv",
        "--output-file",
        "data/jun-dashboard-data.json",
    ]

    expected = Namespace(
        month=6,
        year=2019,
        practice_list_file="data/practice-list.json",
        input_files=["data/jun.csv", "data/july.csv"],
        output_file="data/jun-dashboard-data.json",
    )

    actual = parse_dashboard_pipeline_arguments(args)

    assert actual == expected
