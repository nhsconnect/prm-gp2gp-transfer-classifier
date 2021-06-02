from prmdata.utils.date_anchor import DateAnchor
from tests.builders.common import a_datetime


def test_current_year():
    moment = a_datetime(year=2021)

    date_anchor = DateAnchor(moment)

    expected = "2021"

    actual = date_anchor.current_year

    assert actual == expected


def test_current_month():
    moment = a_datetime(month=11)

    date_anchor = DateAnchor(moment)

    expected = "11"

    actual = date_anchor.current_month

    assert actual == expected
