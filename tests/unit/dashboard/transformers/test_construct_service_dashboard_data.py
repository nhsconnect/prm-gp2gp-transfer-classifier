from datetime import datetime
from freezegun import freeze_time

from gp2gp.dashboard.transformers import construct_service_dashboard_data
from tests.builders.service import build_practice_summary


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42))
def test_dashboard_data_has_correct_generated_on_given_time():
    expected = datetime(year=2019, month=6, day=2, hour=23, second=42)

    actual = construct_service_dashboard_data(sla_metrics=[])

    assert actual.generated_on == expected


@freeze_time(datetime(year=2019, month=4, day=28, hour=23, second=42))
def test_dashboard_data_has_correct_generated_on_given_different_time():
    expected = datetime(year=2019, month=4, day=28, hour=23, second=42)

    actual = construct_service_dashboard_data(sla_metrics=[])

    assert actual.generated_on == expected


def test_dashboard_data_has_correct_ods_given_a_single_practice():
    practice_summaries = [build_practice_summary(ods="A12345")]

    expected_ods = "A12345"

    actual = construct_service_dashboard_data(practice_summaries)

    assert actual.practices[0].ods == expected_ods
