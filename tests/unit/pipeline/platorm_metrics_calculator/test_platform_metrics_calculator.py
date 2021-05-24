from prmdata.pipeline.platform_metrics_calculator.main_new import (
    generate_s3_path_for_input_data,
    generate_s3_path,
)


def test_should_generate_s3_path_for_spine_input_files():
    expected_input_path = "s3://bucket-name/v2/2020/1/Jan-2020.csv.gz"
    expected_input_overflow_path = "s3://bucket-name/v2/2020/2/overflow/Feb-2020.csv.gz"

    input_bucket = "bucket-name"
    month = 1
    year = 2020

    input_path, input_overflow_path = generate_s3_path_for_input_data(year, month, input_bucket)

    assert input_path == expected_input_path
    assert input_overflow_path == expected_input_overflow_path


def test_should_generate_s3_path_for_spine_input_files_over_newyears():
    expected_input_path = "s3://bucket-name/v2/2019/12/Dec-2019.csv.gz"
    expected_input_overflow_path = "s3://bucket-name/v2/2020/1/overflow/Jan-2020.csv.gz"

    input_bucket = "bucket-name"
    month = 12
    year = 2019

    input_path, input_overflow_path = generate_s3_path_for_input_data(year, month, input_bucket)

    assert input_path == expected_input_path
    assert input_overflow_path == expected_input_overflow_path


def test_should_generate_s3_output_path_for_a_specific_filename():
    expected_s3_path = "s3://bucket-name/v2/2020/1/something.json"

    bucket_name = "bucket-name"
    month = 1
    year = 2020
    file_name = "something.json"

    s3_path = generate_s3_path(year, month, bucket_name, file_name)

    assert s3_path == expected_s3_path
