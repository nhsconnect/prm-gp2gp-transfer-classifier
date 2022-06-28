from prmdata.utils.filters import find_first


def test_find_first_matching_a_condition():
    a_dictionary = {"key": "matching_value"}

    a_list = [{"key": "1"}, a_dictionary, {"key": "3"}]
    expected_result = a_dictionary

    matching_condition = find_first(
        iterable=a_list, default=None, condition=lambda x: x["key"] == "matching_value"
    )

    assert matching_condition == expected_result


def test_find_first_returns_specific_dictionary_value_matching_a_condition():
    a_dictionary = {"key": "matching_value", "other_key": "a_value"}

    a_list = [{"key": "1"}, a_dictionary, {"key": "3"}]
    expected_result = "a_value"

    matching_condition = find_first(
        iterable=a_list,
        value=lambda x: x["other_key"],
        default=None,
        condition=lambda x: x["key"] == "matching_value",
    )

    assert matching_condition == expected_result


def test_find_first_uses_defined_default_if_condition_not_met():
    a_list = ["1", "2", "3"]

    default_value = 1
    expected_result = default_value

    matching_condition = find_first(
        iterable=a_list, default=default_value, condition=lambda x: x == "non_matching_value"
    )

    assert matching_condition == expected_result


def test_find_first_uses_default_none_if_condition_not_met():
    a_list = ["1", "2", "3"]

    matching_condition = find_first(iterable=a_list, condition=lambda x: x == "non_matching_value")

    assert matching_condition is None
