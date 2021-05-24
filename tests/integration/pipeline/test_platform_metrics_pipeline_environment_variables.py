from datetime import datetime

from prmdata.pipeline.platform_metrics_calculator.config import (
    DataPipelineConfig,
    MissingEnvironmentVariable,
)


def test_read_config_from_environment_when_optional_parameters_are_not_set():
    environment = {
        "OUTPUT_BUCKET": "output-bucket",
        "INPUT_BUCKET": "mapping-bucket",
        "ORGANISATION_LIST_BUCKET": "",
    }

    expected_config = DataPipelineConfig(
        input_bucket="mapping-bucket",
        output_bucket="output-bucket",
        organisation_list_bucket="",
        year=datetime.utcnow().year,
        month=datetime.utcnow().month,
        s3_endpoint_url=None,
    )

    actual_config = DataPipelineConfig.from_environment_variables(environment)

    assert actual_config == expected_config


def test_from_environment_variables_converts_year_month_to_integer():
    environment = {
        "OUTPUT_BUCKET": "output-bucket",
        "INPUT_BUCKET": "mapping-bucket",
        "ORGANISATION_LIST_BUCKET": "",
        "YEAR": "2020",
        "MONTH": "01",
    }

    expected_config = DataPipelineConfig(
        input_bucket="mapping-bucket",
        output_bucket="output-bucket",
        organisation_list_bucket="",
        year=2020,
        month=1,
    )

    actual_config = DataPipelineConfig.from_environment_variables(environment)

    assert actual_config == expected_config


def test_error_from_environment_when_required_fields_are_not_set():
    environment = {
        "OUTPUT_BUCKET": "output-bucket",
        "INPUT_BUCKET": "mapping-bucket",
    }

    try:
        DataPipelineConfig.from_environment_variables(environment)
    except MissingEnvironmentVariable as ex:
        assert (
            str(ex)
            == "Expected environment variable ORGANISATION_LIST_BUCKET was not set, exiting..."
        )
