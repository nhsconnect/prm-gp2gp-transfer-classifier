from datetime import datetime, timedelta

import pytest
from dateutil.tz import tzutc

from prmdata.pipeline.config import MissingEnvironmentVariable, TransferClassifierConfig


def test_reads_from_environment_variables_and_converts_to_required_format():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_SPINE_DATA_BUCKET": "input-spine-data-bucket",
        "INPUT_ODS_METADATA_BUCKET": "input-ods-metadata-bucket",
        "INPUT_MI_DATA_BUCKET": "input-mi-data-bucket",
        "START_DATETIME": "2020-01-30T00:00:00Z",
        "END_DATETIME": "2020-01-30T23:59:59Z",
        "CONVERSATION_CUTOFF_DAYS": "28",
        "S3_ENDPOINT_URL": "a_url",
        "BUILD_TAG": "12345",
        "CLASSIFY_MI_EVENTS": "True",
    }

    expected_config = TransferClassifierConfig(
        input_spine_data_bucket="input-spine-data-bucket",
        output_transfer_data_bucket="output-transfer-data-bucket",
        input_ods_metadata_bucket="input-ods-metadata-bucket",
        input_mi_data_bucket="input-mi-data-bucket",
        start_datetime=datetime(
            year=2020, month=1, day=30, hour=00, minute=00, second=00, tzinfo=tzutc()
        ),
        end_datetime=datetime(
            year=2020, month=1, day=30, hour=23, minute=59, second=59, tzinfo=tzutc()
        ),
        conversation_cutoff=timedelta(days=28),
        s3_endpoint_url="a_url",
        build_tag="12345",
        classify_mi_events=True,
    )

    actual_config = TransferClassifierConfig.from_environment_variables(environment)

    assert actual_config == expected_config


def test_read_config_from_environment_when_optional_parameters_are_not_set():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_SPINE_DATA_BUCKET": "input-spine-data-bucket",
        "INPUT_ODS_METADATA_BUCKET": "input-ods-metadata-bucket",
        "BUILD_TAG": "12345",
    }

    expected_config = TransferClassifierConfig(
        input_spine_data_bucket="input-spine-data-bucket",
        output_transfer_data_bucket="output-transfer-data-bucket",
        input_ods_metadata_bucket="input-ods-metadata-bucket",
        input_mi_data_bucket=None,
        start_datetime=None,
        end_datetime=None,
        s3_endpoint_url=None,
        conversation_cutoff=timedelta(days=0),
        build_tag="12345",
        classify_mi_events=False,
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


def test_returns_valid_config_given_environment_variable_cutoff_is_0():
    environment = {
        "OUTPUT_TRANSFER_DATA_BUCKET": "output-transfer-data-bucket",
        "INPUT_SPINE_DATA_BUCKET": "input-spine-data-bucket",
        "INPUT_ODS_METADATA_BUCKET": "input-ods-metadata-bucket",
        "BUILD_TAG": "12345",
        "CONVERSATION_CUTOFF_DAYS": "0",
    }

    expected_config = TransferClassifierConfig(
        input_spine_data_bucket="input-spine-data-bucket",
        output_transfer_data_bucket="output-transfer-data-bucket",
        input_ods_metadata_bucket="input-ods-metadata-bucket",
        input_mi_data_bucket=None,
        start_datetime=None,
        end_datetime=None,
        s3_endpoint_url=None,
        conversation_cutoff=timedelta(days=0),
        build_tag="12345",
        classify_mi_events=False,
    )

    actual_config = TransferClassifierConfig.from_environment_variables(environment)

    assert actual_config == expected_config
