from unittest.mock import patch

from gp2gp.pipeline.dashboard.args import parse_dashboard_pipeline_arguments

from argparse import Namespace


def test_parse_arguments_with_local_files():
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
        output_bucket=None,
        practice_metrics_output_key=None,
        practice_metadata_output_key=None,
        s3_endpoint_url=None,
    )

    actual = parse_dashboard_pipeline_arguments(args)

    assert actual == expected


def test_parse_arguments_with_s3_upload():
    args = [
        "--month",
        "6",
        "--year",
        "2019",
        "--practice-list-file",
        "data/practice-list.json",
        "--input-files",
        "data/jun.csv,data/july.csv",
        "--output-bucket",
        "test-bucket",
        "--practice-metrics-output-key",
        "jun-practice-metrics.json",
        "--practice-metadata-output-key",
        "jun-practice-metadata.json",
        "--s3-endpoint-url",
        "https://localhost:6789",
    ]

    expected = Namespace(
        month=6,
        year=2019,
        practice_list_file="data/practice-list.json",
        input_files=["data/jun.csv", "data/july.csv"],
        output_bucket="test-bucket",
        practice_metrics_output_key="jun-practice-metrics.json",
        practice_metadata_output_key="jun-practice-metadata.json",
        s3_endpoint_url="https://localhost:6789",
        practice_metrics_output_file=None,
        practice_metadata_output_file=None,
    )

    actual = parse_dashboard_pipeline_arguments(args)

    assert actual == expected


@patch("sys.exit")
def test_fails_when_key_supplied_but_no_bucket_supplied(mock_exit):
    args = [
        "--month",
        "6",
        "--year",
        "2019",
        "--practice-list-file",
        "data/practice-list.json",
        "--input-files",
        "data/jun.csv,data/july.csv",
        "--practice-metrics-output-key",
        "jun-practice-metrics.json",
        "--practice-metadata-output-key",
        "jun-practice-metadata.json",
    ]

    parse_dashboard_pipeline_arguments(args)

    mock_exit.assert_called_with(2)
