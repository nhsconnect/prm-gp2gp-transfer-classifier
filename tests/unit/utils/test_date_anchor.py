from datetime import datetime

from prmdata.utils.date_anchor import DateAnchor


def test_current_month():
    moment = datetime(2021, 3, 4)

    date_anchor = DateAnchor(moment)

    expected = datetime(2021, 3, 1)

    actual = date_anchor.current_month

    assert actual == expected


def test_previous_month():
    moment = datetime(2021, 3, 4)

    date_anchor = DateAnchor(moment)

    expected = datetime(2021, 2, 1)

    actual = date_anchor.previous_month

    assert actual == expected


def test_previous_month_path_over_new_year():
    moment = datetime(2021, 1, 4)

    date_anchor = DateAnchor(moment)

    expected = datetime(2020, 12, 1)

    actual = date_anchor.previous_month

    assert actual == expected
