from dataclasses import dataclass
from datetime import datetime
from typing import List

from gp2gp.io.write_as_json import serialize_as_json


def test_serialize_single_string_property():
    @dataclass
    class PracticeDetailTest:
        ods_code: str

    practice_detail = PracticeDetailTest(ods_code="A12345")

    actual = serialize_as_json(practice_detail)

    expected = '{"odsCode": "A12345"}'

    assert actual == expected


def test_serialize_single_datetime_property():
    @dataclass
    class PracticeDetailTest:
        generated_on: datetime

    practice_detail = PracticeDetailTest(generated_on=datetime(2020, 7, 23))

    actual = serialize_as_json(practice_detail)

    expected = '{"generatedOn": "2020-07-23T00:00:00"}'

    assert actual == expected


def test_serialize_single_list_property():
    @dataclass
    class PracticeDetailTest:
        ods_code: str

    @dataclass
    class PracticeListTest:
        practices: List[PracticeDetailTest]

    practices = PracticeListTest(
        practices=[PracticeDetailTest(ods_code="A12345"), PracticeDetailTest(ods_code="B12345")]
    )

    actual = serialize_as_json(practices)

    expected = '{"practices": [{"odsCode": "A12345"}, {"odsCode": "B12345"}]}'

    assert actual == expected


def test_serialize_nested_property():
    @dataclass
    class SlaMetricTest:
        within_3_days: int

    @dataclass
    class PracticeDetailTest:
        ods_code: str
        sla_metrics: SlaMetricTest

    @dataclass
    class PracticeNestingTest:
        practice: PracticeDetailTest

    practice = PracticeNestingTest(
        practice=PracticeDetailTest(ods_code="A12345", sla_metrics=SlaMetricTest(within_3_days=5))
    )

    actual = serialize_as_json(practice)

    expected = '{"practice": {"odsCode": "A12345", "slaMetrics": {"within3Days": 5}}}'

    assert actual == expected
