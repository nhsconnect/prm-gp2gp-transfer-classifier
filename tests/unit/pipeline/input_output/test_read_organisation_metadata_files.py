from unittest.mock import Mock, call

from prmdata.domain.ods_portal.organisation_lookup import OrganisationLookup
from prmdata.pipeline.io import TransferClassifierIO
from tests.builders.common import a_string

_DATE_ANCHOR_YEAR = 2021
_DATE_ANCHOR_MONTH = 1
_DATE_ANCHOR_ADDITIONAL_MONTH = 2

_DATE_ANCHOR_YEAR_MONTH = (_DATE_ANCHOR_YEAR, _DATE_ANCHOR_MONTH)
_DATE_ANCHOR_ADDITIONAL_YEAR_MONTH = (_DATE_ANCHOR_YEAR, _DATE_ANCHOR_ADDITIONAL_MONTH)

_ORGANISATION_METADATA_DICT_FIRST_MONTH = {
    "generated_on": "2021-02-15T00:00:00.000000+00:00",
    "year": 2021,
    "month": 1,
    "practices": [{"ods_code": "ABC", "name": "A Practice", "asids": ["123"]}],
    "sicbls": [{"ods_code": "XYZ", "name": "A SICBL", "practices": ["ABC"]}],
}
_ORGANISATION_METADATA_DICT_ADDITIONAL_MONTH = {
    "generated_on": "2021-03-15T03:00:00",
    "year": 2021,
    "month": 2,
    "practices": [{"ods_code": "A12345", "name": "GP Practice", "asids": ["123456789123"]}],
    "sicbls": [{"ods_code": "22A", "name": "SICBL", "practices": ["A12345"]}],
}

_ODS_BUCKET = a_string()
_S3_PREFIX = f"s3://{_ODS_BUCKET}/v2"
_S3_URI = (
    f"{_S3_PREFIX}/{_DATE_ANCHOR_YEAR}/{_DATE_ANCHOR_ADDITIONAL_MONTH}/organisationMetadata.json"
)
_S3_URI_ADDITIONAL_MONTH = (
    f"{_S3_PREFIX}/{_DATE_ANCHOR_YEAR}/{_DATE_ANCHOR_ADDITIONAL_MONTH}/organisationMetadata.json"
)


def test_convert_one_organisation_metadata_file_to_organisation_lookup():
    s3_manager = Mock()
    s3_manager.read_json.return_value = _ORGANISATION_METADATA_DICT_FIRST_MONTH

    transfer_classifier_io = TransferClassifierIO(s3_data_manager=s3_manager)
    actual_metadatas = transfer_classifier_io.read_ods_metadata_files(s3_uris=[_S3_URI])
    actual_organisation_lookup = actual_metadatas.get_lookup(_DATE_ANCHOR_YEAR_MONTH)

    expected_organisation_lookup_month = _DATE_ANCHOR_MONTH

    assert actual_organisation_lookup.month == expected_organisation_lookup_month
    assert isinstance(actual_organisation_lookup, OrganisationLookup)

    s3_manager.read_json.assert_called_once_with(_S3_URI)


def test_convert_previous_month_organisation_metadata_file_to_organisation_lookup():
    s3_manager = Mock()
    s3_manager.read_json.return_value = _ORGANISATION_METADATA_DICT_FIRST_MONTH

    transfer_classifier_io = TransferClassifierIO(s3_data_manager=s3_manager)

    actual_metadatas = transfer_classifier_io.read_ods_metadata_files(s3_uris=[_S3_URI])
    actual_organisation_lookup = actual_metadatas.get_lookup(_DATE_ANCHOR_ADDITIONAL_YEAR_MONTH)

    expected_organisation_lookup_month = _DATE_ANCHOR_MONTH

    assert actual_organisation_lookup.month == expected_organisation_lookup_month
    assert isinstance(actual_organisation_lookup, OrganisationLookup)

    s3_manager.read_json.assert_called_once_with(_S3_URI)


def test_convert_two_organisation_metadata_files_to_organisation_lookup_mapping():
    s3_manager = Mock()
    s3_manager.read_json.side_effect = [
        _ORGANISATION_METADATA_DICT_FIRST_MONTH,
        _ORGANISATION_METADATA_DICT_ADDITIONAL_MONTH,
    ]

    transfer_classifier_io = TransferClassifierIO(s3_data_manager=s3_manager)
    actual_metadatas = transfer_classifier_io.read_ods_metadata_files(
        s3_uris=[_S3_URI, _S3_URI_ADDITIONAL_MONTH]
    )
    actual_first_month_organisation_lookup = actual_metadatas.get_lookup(_DATE_ANCHOR_YEAR_MONTH)

    expected_first_month_organisation_lookup_month = _DATE_ANCHOR_MONTH

    assert (
        actual_first_month_organisation_lookup.month
        == expected_first_month_organisation_lookup_month
    )
    assert isinstance(actual_first_month_organisation_lookup, OrganisationLookup)

    actual_second_organisation_lookup = actual_metadatas.get_lookup(
        _DATE_ANCHOR_ADDITIONAL_YEAR_MONTH
    )

    expected_second_organisation_lookup_month = _DATE_ANCHOR_ADDITIONAL_MONTH

    assert actual_second_organisation_lookup.month == expected_second_organisation_lookup_month
    assert isinstance(actual_second_organisation_lookup, OrganisationLookup)

    expected_s3_manager_read_json_calls = [call(_S3_URI), call(_S3_URI_ADDITIONAL_MONTH)]
    s3_manager.read_json.assert_has_calls(expected_s3_manager_read_json_calls)
