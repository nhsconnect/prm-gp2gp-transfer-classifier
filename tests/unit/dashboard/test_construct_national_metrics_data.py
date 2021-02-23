from datetime import datetime

import pytest
from dateutil.tz import tzutc
from freezegun import freeze_time

from gp2gp.dashboard.national_data import construct_national_data_platform_data
from gp2gp.service.national_metrics_by_month import NationalMetricsByMonth, IntegratedMetrics
from tests.builders.common import an_integer, a_datetime


def build_national_metrics_by_month(**kwargs) -> NationalMetricsByMonth:
    within_3_days = kwargs.get("within_3_days", an_integer())
    within_8_days = kwargs.get("within_8_days", an_integer())
    beyond_8_days = kwargs.get("beyond_8_days", an_integer())
    summed_transfer_count = within_3_days + within_8_days + beyond_8_days

    return NationalMetricsByMonth(
        year=a_datetime().year,
        month=a_datetime().month,
        transfer_count=kwargs.get("transfer_count", summed_transfer_count),
        integrated=IntegratedMetrics(
            transfer_count=kwargs.get("integrated_transfer_count", summed_transfer_count),
            within_3_days=within_3_days,
            within_8_days=within_8_days,
            beyond_8_days=beyond_8_days,
        ),
    )


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42), tz_offset=0)
def test_has_correct_generated_on_given_time():
    expected_generated_on = datetime(year=2019, month=6, day=2, hour=23, second=42, tzinfo=tzutc())
    national_metrics_by_month = build_national_metrics_by_month()
    actual = construct_national_data_platform_data(national_metrics_by_month)

    assert actual.generated_on == expected_generated_on


def test_has_transfer_count_of_all_transfers():
    expected_transfer_count = an_integer(2, 7)
    national_metrics_by_month = build_national_metrics_by_month(
        transfer_count=expected_transfer_count
    )
    actual = construct_national_data_platform_data(national_metrics_by_month)

    assert actual.metrics[0].transfer_count == expected_transfer_count


def test_has_integrated_transfer_count():
    expected_integrated_transfer_count = an_integer(2, 7)
    national_metrics_by_month = build_national_metrics_by_month(
        integrated_transfer_count=expected_integrated_transfer_count
    )
    actual = construct_national_data_platform_data(national_metrics_by_month)

    assert actual.metrics[0].integrated.transfer_count == expected_integrated_transfer_count


@pytest.mark.parametrize(
    "national_metrics_integrated",
    [
        ({"within_3_days": 0, "within_8_days": 0, "beyond_8_days": 0}),
        ({"within_3_days": 1, "within_8_days": 2, "beyond_8_days": 3}),
        ({"within_3_days": 2, "within_8_days": 0, "beyond_8_days": 0}),
        ({"within_3_days": 0, "within_8_days": 2, "beyond_8_days": 0}),
        ({"within_3_days": 0, "within_8_days": 0, "beyond_8_days": 2}),
    ],
)
def test_returns_integrated_transfer_count_by_sla_duration(national_metrics_integrated):
    national_metrics_by_month = build_national_metrics_by_month(
        within_3_days=national_metrics_integrated["within_3_days"],
        within_8_days=national_metrics_integrated["within_8_days"],
        beyond_8_days=national_metrics_integrated["beyond_8_days"],
    )
    actual_integrated_metrics = (
        construct_national_data_platform_data(national_metrics_by_month).metrics[0].integrated
    )

    assert actual_integrated_metrics.within_3_days == national_metrics_integrated["within_3_days"]
    assert actual_integrated_metrics.within_8_days == national_metrics_integrated["within_8_days"]
    assert actual_integrated_metrics.beyond_8_days == national_metrics_integrated["beyond_8_days"]


def test_has_integrated_percentage():
    expected_transfer_count = 3
    national_metrics_by_month = build_national_metrics_by_month(
        transfer_count=expected_transfer_count, integrated_transfer_count=1
    )
    expected_percentage = 33.33
    actual = construct_national_data_platform_data(national_metrics_by_month)

    assert actual.metrics[0].integrated.transfer_percentage == expected_percentage


def test_has_paper_fallback_transfer_count():
    transfer_count = 10
    national_metrics_by_month = build_national_metrics_by_month(
        transfer_count=transfer_count, within_3_days=5, within_8_days=2, beyond_8_days=1
    )
    actual = construct_national_data_platform_data(national_metrics_by_month)
    expected = 3

    assert actual.metrics[0].paper_fallback.transfer_count == expected
