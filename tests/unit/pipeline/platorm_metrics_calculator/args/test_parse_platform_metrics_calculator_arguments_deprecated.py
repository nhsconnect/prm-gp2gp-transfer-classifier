from prmdata.pipeline.platform_metrics_calculator.args import (
    parse_platform_metrics_calculator_pipeline_arguments,
)

from argparse import Namespace

# TODO: Remove with PRMT-1798


def test_parse_arguments_with_local_files():
    args = [
        "--month",
        "6",
        "--year",
        "2019",
        "--organisation-list-file",
        "data/organisationMetadata.json",
        "--input-files",
        "data/jun.csv,data/july.csv",
        "--output-directory",
        "data",
    ]

    expected = Namespace(
        month=6,
        year=2019,
        organisation_list_file="data/organisationMetadata.json",
        input_files=["data/jun.csv", "data/july.csv"],
        output_bucket=None,
        output_directory="data",
        s3_endpoint_url=None,
    )

    actual = parse_platform_metrics_calculator_pipeline_arguments(args)

    assert actual == expected


def test_parse_arguments_with_s3_upload():
    args = [
        "--month",
        "6",
        "--year",
        "2019",
        "--organisation-list-file",
        "data/organisationMetadata.json",
        "--input-files",
        "data/jun.csv,data/july.csv",
        "--output-bucket",
        "test-bucket",
        "--s3-endpoint-url",
        "https://localhost:6789",
    ]

    expected = Namespace(
        month=6,
        year=2019,
        organisation_list_file="data/organisationMetadata.json",
        input_files=["data/jun.csv", "data/july.csv"],
        output_bucket="test-bucket",
        output_directory=None,
        s3_endpoint_url="https://localhost:6789",
    )

    actual = parse_platform_metrics_calculator_pipeline_arguments(args)

    assert actual == expected
