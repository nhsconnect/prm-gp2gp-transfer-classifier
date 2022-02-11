from prmdata.domain.ods_portal.organisation_metadata import OrganisationMetadata
from prmdata.domain.ods_portal.organisation_metadatas import OrganisationMetadataLookup


def test_from_list_of_data_returns_dict_of_organisation_metadata():
    first_month_data = {
        "generated_on": "2020-07-23T00:00:00",
        "practices": [{"ods_code": "A12345", "name": "GP Practice", "asids": ["123456789123"]}],
        "ccgs": [{"ods_code": "12A", "name": "CCG", "practices": ["A12345"]}],
    }
    second_month_data = {
        "generated_on": "2020-08-23T00:00:00",
        "practices": [{"ods_code": "A12345", "name": "GP Practice", "asids": ["123456789123"]}],
        "ccgs": [{"ods_code": "22A", "name": "CCG", "practices": ["A12345"]}],
    }
    list_of_data = iter([first_month_data, second_month_data])

    expected_first_month_metadata = OrganisationMetadata.from_dict(first_month_data)
    expected_second_month_metadata = OrganisationMetadata.from_dict(second_month_data)
    actual_metadata_lookup = OrganisationMetadataLookup.from_list(list_of_data)

    actual_first_month_metadata = actual_metadata_lookup.get_month_metadata((2020, 7))
    actual_second_month_metadata = actual_metadata_lookup.get_month_metadata((2020, 8))

    assert actual_first_month_metadata == expected_first_month_metadata
    assert actual_second_month_metadata == expected_second_month_metadata
