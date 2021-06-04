from datetime import datetime

from dateutil.tz import tzutc

from prmdata.pipeline.platform_metrics_calculator.config import (
    DataPipelineConfig,
    MissingEnvironmentVariable,
)


def test_reads_from_environment_variables_and_converts_to_required_format():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_TRANSFER_DATA_BUCKET": "input-transfer-data-bucket",
        "ORGANISATION_METADATA_BUCKET": "",
        "DATE_ANCHOR": "2020-01-30T18:44:49Z",
    }

    expected_config = DataPipelineConfig(
        input_transfer_data_bucket="input-transfer-data-bucket",
        output_transfer_data_bucket="output-transfer-data-bucket",
        organisation_metadata_bucket="",
        date_anchor=datetime(
            year=2020, month=1, day=30, hour=18, minute=44, second=49, tzinfo=tzutc()
        ),
    )

    actual_config = DataPipelineConfig.from_environment_variables(environment)

    assert actual_config == expected_config


def test_read_config_from_environment_when_optional_parameters_are_not_set():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_TRANSFER_DATA_BUCKET": "input-transfer-data-bucket",
        "ORGANISATION_METADATA_BUCKET": "",
        "DATE_ANCHOR": "2020-01-30T18:44:49Z",
    }

    expected_config = DataPipelineConfig(
        input_transfer_data_bucket="input-transfer-data-bucket",
        output_transfer_data_bucket="output-transfer-data-bucket",
        organisation_metadata_bucket="",
        date_anchor=datetime(
            year=2020, month=1, day=30, hour=18, minute=44, second=49, tzinfo=tzutc()
        ),
        s3_endpoint_url=None,
    )

    actual_config = DataPipelineConfig.from_environment_variables(environment)

    assert actual_config == expected_config


def test_error_from_environment_when_required_fields_are_not_set():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_TRANSFER_DATA_BUCKET": "input-transfer-data-bucket",
    }

    try:
        DataPipelineConfig.from_environment_variables(environment)
    except MissingEnvironmentVariable as ex:
        assert (
            str(ex)
            == "Expected environment variable ORGANISATION_METADATA_BUCKET was not set, exiting..."
        )
