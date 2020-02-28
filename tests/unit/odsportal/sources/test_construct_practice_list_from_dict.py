from datetime import datetime

from gp2gp.odsportal.models import PracticeDetails
from gp2gp.odsportal.sources import construct_practice_list_from_dict


def test_returns_model_with_generated_on_timestamp():
    data = {"generated_on": "2020-07-23T00:00:00", "practices": []}

    expected_timestamp = datetime(2020, 7, 23)
    actual = construct_practice_list_from_dict(data)

    assert actual.generated_on == expected_timestamp


def test_returns_list_with_one_practice():
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "practices": [{"ods_code": "A12345", "name": "GP Practice"}],
    }

    expected_practices = [PracticeDetails(ods_code="A12345", name="GP Practice")]
    actual = construct_practice_list_from_dict(data)

    assert actual.practices == expected_practices


def test_returns_list_with_multiple_practices():
    data = {
        "generated_on": "2020-07-23T00:00:00",
        "practices": [
            {"ods_code": "A12345", "name": "GP Practice"},
            {"ods_code": "B12345", "name": "GP Practice 2"},
            {"ods_code": "C12345", "name": "GP Practice 3"},
        ],
    }

    expected_practices = [
        PracticeDetails(ods_code="A12345", name="GP Practice"),
        PracticeDetails(ods_code="B12345", name="GP Practice 2"),
        PracticeDetails(ods_code="C12345", name="GP Practice 3"),
    ]
    actual = construct_practice_list_from_dict(data)

    assert actual.practices == expected_practices
