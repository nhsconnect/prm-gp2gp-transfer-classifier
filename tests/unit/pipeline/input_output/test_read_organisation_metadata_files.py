from unittest.mock import Mock

from prmdata.domain.ods_portal.organisation_metadata_lookup import OrganisationMetadataLookup
from prmdata.pipeline.io import TransferClassifierIO
from tests.builders.common import a_string

_DATE_ANCHOR_MONTH = 1
_DATE_ANCHOR_YEAR = 2021

_ORGANISATION_METADATA_DICT = {
    "generated_on": "2021-01-01T00:00:00.000000+00:00",
    "practices": [{"ods_code": "ABC", "name": "A Practice", "asids": ["123"]}],
    "ccgs": [{"ods_code": "XYZ", "name": "A CCG", "practices": ["ABC"]}],
}


def test_convert_one_organisation_metadata_file_to_organisation_metadata_lookup():
    s3_manager = Mock()
    ods_bucket = a_string()
    s3_uri = (
        f"s3://{ods_bucket}/v2/{_DATE_ANCHOR_YEAR}/{_DATE_ANCHOR_MONTH}/organisationMetadata.json"
    )

    transfer_classifier_io = TransferClassifierIO(s3_data_manager=s3_manager)

    s3_manager.read_json.return_value = _ORGANISATION_METADATA_DICT

    date_anchor_year_month = (_DATE_ANCHOR_YEAR, _DATE_ANCHOR_MONTH)

    actual_metadata_lookup = transfer_classifier_io.read_ods_metadata_files(s3_uris=[s3_uri])
    actual_month_metadata = actual_metadata_lookup.get_month_metadata(date_anchor_year_month)

    expected_metadata_lookup = OrganisationMetadataLookup.from_list(
        iter([_ORGANISATION_METADATA_DICT])
    )
    expected_month_metadata = expected_metadata_lookup.get_month_metadata(date_anchor_year_month)

    assert actual_month_metadata == expected_month_metadata

    s3_manager.read_json.assert_called_once_with(s3_uri)
