from datetime import datetime, timedelta

import pytest
from dateutil.tz import tzutc

from prmdata.pipeline.config import TransferClassifierConfig, MissingEnvironmentVariable


def test_reads_from_environment_variables_and_converts_to_required_format():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_SPINE_DATA_BUCKET": "input-spine-data-bucket",
        "DATE_ANCHOR": "2020-01-30T18:44:49Z",
        "CONVERSATION_CUTOFF_DAYS": "28",
        "S3_ENDPOINT_URL": "a_url",
        "BUILD_TAG": "12345",
    }

    expected_config = TransferClassifierConfig(
        input_spine_data_bucket="input-spine-data-bucket",
        output_transfer_data_bucket="output-transfer-data-bucket",
        date_anchor=datetime(
            year=2020, month=1, day=30, hour=18, minute=44, second=49, tzinfo=tzutc()
        ),
        conversation_cutoff=timedelta(days=28),
        s3_endpoint_url="a_url",
        build_tag="12345",
    )

    actual_config = TransferClassifierConfig.from_environment_variables(environment)

    assert actual_config == expected_config


def test_read_config_from_environment_when_optional_parameters_are_not_set():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_SPINE_DATA_BUCKET": "input-spine-data-bucket",
        "DATE_ANCHOR": "2020-01-30T18:44:49Z",
        "BUILD_TAG": "12345",
    }

    expected_config = TransferClassifierConfig(
        input_spine_data_bucket="input-spine-data-bucket",
        output_transfer_data_bucket="output-transfer-data-bucket",
        date_anchor=datetime(
            year=2020, month=1, day=30, hour=18, minute=44, second=49, tzinfo=tzutc()
        ),
        s3_endpoint_url=None,
        conversation_cutoff=timedelta(days=14),
        build_tag="12345",
    )

    actual_config = TransferClassifierConfig.from_environment_variables(environment)

    assert actual_config == expected_config


def test_error_from_environment_when_required_fields_are_not_set():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
    }

    with pytest.raises(MissingEnvironmentVariable) as e:
        TransferClassifierConfig.from_environment_variables(environment)

    assert (
        str(e.value)
        == "Expected environment variable INPUT_SPINE_DATA_BUCKET was not set, exiting..."
    )
