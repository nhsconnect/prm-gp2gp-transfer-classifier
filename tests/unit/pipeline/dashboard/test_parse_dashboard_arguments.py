from gp2gp.pipeline.dashboard.main import parse_dashboard_pipeline_arguments

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
        "--practice-metrics-output-file",
        "data/jun-practice-metrics.json",
        "--practice-metadata-output-file",
        "data/jun-practice-metadata.json",
    ]

    expected = Namespace(
        month=6,
        year=2019,
        practice_list_file="data/practice-list.json",
        input_files=["data/jun.csv", "data/july.csv"],
        practice_metrics_output_file="data/jun-practice-metrics.json",
        practice_metadata_output_file="data/jun-practice-metadata.json",
    )

    actual = parse_dashboard_pipeline_arguments(args)

    assert actual == expected
