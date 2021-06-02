from prmdata.utils.date_anchor import DateAnchor
from tests.builders.common import a_datetime


def test_current_month():
    moment = a_datetime(year=2021, month=3)

    date_anchor = DateAnchor(moment)

    expected = "2021/3"

    actual = date_anchor.current_month_prefix

    assert actual == expected


def test_previous_month_prefix():
    moment = a_datetime(year=2021, month=3)

    date_anchor = DateAnchor(moment)

    expected = "2021/2"

    actual = date_anchor.previous_month_prefix

    assert actual == expected


def test_previous_month_prefix_over_new_year():
    moment = a_datetime(year=2021, month=1)

    date_anchor = DateAnchor(moment)

    expected = "2020/12"

    actual = date_anchor.previous_month_prefix

    assert actual == expected
