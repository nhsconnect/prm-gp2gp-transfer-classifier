from datetime import timedelta

from prmdata.domain.gp2gp.sla import SlaCounter
from tests.builders.common import a_duration


def test_total_returns_zero_given_no_durations():
    counter = SlaCounter()

    actual = counter.total()

    expected = 0

    assert actual == expected


def test_total_returns_one_given_a_duration():
    counter = SlaCounter()
    duration = a_duration()

    counter.increment(duration)
    actual = counter.total()

    expected = 1

    assert actual == expected


def test_total_returns_two_given_two_durations():
    counter = SlaCounter()
    duration_one = a_duration()
    duration_two = a_duration()

    counter.increment(duration_one)
    counter.increment(duration_two)
    actual = counter.total()

    expected = 2

    assert actual == expected


def test_within_3_days_returns_zero_given_no_durations():
    counter = SlaCounter()

    actual = counter.within_3_days()

    expected = 0

    assert actual == expected


def test_within_3_days_only_increments_on_durations_within_three_days():
    counter = SlaCounter()

    duration_one_day = timedelta(days=1)
    duration_two_days = timedelta(days=2)
    duration_four_days = timedelta(days=4)

    counter.increment(duration_one_day)
    counter.increment(duration_two_days)
    counter.increment(duration_four_days)
    actual = counter.within_3_days()

    expected = 2

    assert actual == expected


def test_within_8_days_returns_zero_given_no_durations():
    counter = SlaCounter()

    actual = counter.within_8_days()

    expected = 0

    assert actual == expected


def test_within_8_days_only_increments_on_durations_between_3_and_8_days():
    counter = SlaCounter()

    duration_two_days = timedelta(days=2)
    duration_four_days = timedelta(days=4)
    duration_five_days = timedelta(days=5)
    duration_nine_days = timedelta(days=9)

    counter.increment(duration_two_days)
    counter.increment(duration_four_days)
    counter.increment(duration_five_days)
    counter.increment(duration_nine_days)
    actual = counter.within_8_days()

    expected = 2

    assert actual == expected


def test_beyond_8_days_returns_zero_given_no_durations():
    counter = SlaCounter()

    actual = counter.beyond_8_days()

    expected = 0

    assert actual == expected


def test_beyond_8_days_increments_on_durations_beyond_8_days():
    counter = SlaCounter()

    duration_four_days = timedelta(days=4)
    duration_nine_days = timedelta(days=9)
    duration_ten_days = timedelta(days=10)

    counter.increment(duration_four_days)
    counter.increment(duration_nine_days)
    counter.increment(duration_ten_days)
    actual = counter.beyond_8_days()

    expected = 2

    assert actual == expected
