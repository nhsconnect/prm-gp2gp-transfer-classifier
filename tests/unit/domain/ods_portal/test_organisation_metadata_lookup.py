from prmdata.domain.ods_portal.organisation_lookup import OrganisationLookup
from prmdata.domain.ods_portal.organisation_metadata import CcgDetails, PracticeDetails
from prmdata.domain.ods_portal.organisation_metadata_monthly import OrganisationMetadataMonthly


def test_from_list_of_data_returns_dict_of_organisation_metadata():
    first_month_data = {
        "generated_on": "2020-07-23T00:00:00",
        "practices": [{"ods_code": "A12345", "name": "GP Practice", "asids": ["123456789123"]}],
        "ccgs": [{"ods_code": "12A", "name": "CCG", "practices": ["A12345"]}],
    }
    second_month_data = {
        "generated_on": "2020-08-23T00:00:00",
        "practices": [{"ods_code": "ABC543", "name": "GP Practice", "asids": ["123456789123"]}],
        "ccgs": [{"ods_code": "22A", "name": "CCG", "practices": ["A12345"]}],
    }
    list_of_data = iter([first_month_data, second_month_data])

    expected_first_month_practices = [
        PracticeDetails(ods_code="A12345", name="GP Practice", asids=["123456789123"])
    ]
    expected_second_month_practices = [
        PracticeDetails(ods_code="ABC543", name="GP Practice", asids=["123456789123"])
    ]
    expected_first_month_ccgs = [CcgDetails(ods_code="12A", name="CCG", practices=["A12345"])]
    expected_second_month_ccgs = [CcgDetails(ods_code="22A", name="CCG", practices=["A12345"])]
    expected_first_month_lookup = OrganisationLookup(
        expected_first_month_practices, expected_first_month_ccgs
    )
    expected_second_month_lookup = OrganisationLookup(
        expected_second_month_practices, expected_second_month_ccgs
    )

    actual_metadatas = OrganisationMetadataMonthly.from_list(list_of_data)
    actual_first_month_lookup = actual_metadatas.get_lookup((2020, 7))
    actual_second_month_lookup = actual_metadatas.get_lookup((2020, 8))

    assert actual_first_month_lookup.practice_ods_code_from_asid(
        "123456789123"
    ) == expected_first_month_lookup.practice_ods_code_from_asid("123456789123")
    assert actual_second_month_lookup.practice_ods_code_from_asid(
        "123456789123"
    ) == expected_second_month_lookup.practice_ods_code_from_asid("123456789123")
