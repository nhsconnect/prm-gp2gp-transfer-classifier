from unittest.mock import Mock

from dateutil.tz import tzutc
from datetime import datetime
from freezegun import freeze_time

from prmdata.domain.data_platform.practice_metrics import construct_practice_metrics_presentation


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42), tz_offset=0)
def test_practice_has_correct_generated_on_given_time():
    practice_summaries = []

    expected_generated_on = datetime(year=2019, month=6, day=2, hour=23, second=42, tzinfo=tzutc())

    actual = construct_practice_metrics_presentation(practice_summaries)

    assert actual.generated_on == expected_generated_on


def test_practice_has_correct_practice_summaries():
    practice_summaries = [Mock(), Mock()]

    actual = construct_practice_metrics_presentation(practice_summaries)

    assert actual.practices == practice_summaries
