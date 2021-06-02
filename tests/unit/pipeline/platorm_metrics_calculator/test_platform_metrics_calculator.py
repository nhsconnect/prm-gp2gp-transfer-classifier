from prmdata.pipeline.platform_metrics_calculator.main import (
    generate_s3_path_for_input_transfer_data,
)


def test_should_generate_s3_path_for_spine_input_files():
    expected_input_path = "s3://bucket-name/v2/2020/1/Jan-2020.csv.gz"
    expected_input_overflow_path = "s3://bucket-name/v2/2020/2/overflow/Feb-2020.csv.gz"

    input_bucket = "bucket-name"
    month = 2
    year = 2020

    input_path, input_overflow_path = generate_s3_path_for_input_transfer_data(
        year, month, input_bucket
    )

    assert input_path == expected_input_path
    assert input_overflow_path == expected_input_overflow_path


def test_should_generate_s3_path_for_spine_input_files_over_newyears():
    expected_input_path = "s3://bucket-name/v2/2019/12/Dec-2019.csv.gz"
    expected_input_overflow_path = "s3://bucket-name/v2/2020/1/overflow/Jan-2020.csv.gz"

    input_bucket = "bucket-name"
    month = 1
    year = 2020

    input_path, input_overflow_path = generate_s3_path_for_input_transfer_data(
        year, month, input_bucket
    )

    assert input_path == expected_input_path
    assert input_overflow_path == expected_input_overflow_path
