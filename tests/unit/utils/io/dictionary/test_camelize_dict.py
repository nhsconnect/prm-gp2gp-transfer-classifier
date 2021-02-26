from prmdata.utils.io.dictionary import camelize_dict


def test_single_string_property():
    input_dict = {"ods_code": "A12345"}

    actual = camelize_dict(input_dict)

    expected = {"odsCode": "A12345"}

    assert actual == expected


def test_single_list_property():
    input_dict = {"practices": [{"ods_code": "A12345"}, {"ods_code": "B12345"}]}

    actual = camelize_dict(input_dict)

    expected = {"practices": [{"odsCode": "A12345"}, {"odsCode": "B12345"}]}

    assert actual == expected


def test_nested_property():
    input_dict = {"practice": {"ods_code": "A12345", "sla_metrics": {"within_3_days": 5}}}

    actual = camelize_dict(input_dict)

    expected = {"practice": {"odsCode": "A12345", "slaMetrics": {"within3Days": 5}}}

    assert actual == expected
