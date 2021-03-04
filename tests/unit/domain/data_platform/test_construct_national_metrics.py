from datetime import datetime

import pytest
from dateutil.tz import tzutc
from freezegun import freeze_time

from prmdata.domain.data_platform.national_metrics import construct_national_metrics
from prmdata.domain.gp2gp.national_metrics import NationalMetrics, IntegratedMetrics
from tests.builders.common import an_integer, a_datetime

a_year = a_datetime().year
a_month = a_datetime().month


def _build_national_metrics(**kwargs) -> NationalMetrics:
    within_3_days = kwargs.get("within_3_days", an_integer())
    within_8_days = kwargs.get("within_8_days", an_integer())
    beyond_8_days = kwargs.get("beyond_8_days", an_integer())
    summed_transfer_count = within_3_days + within_8_days + beyond_8_days

    return NationalMetrics(
        initiated_transfers_count=kwargs.get("initiated_transfers_count", summed_transfer_count),
        pending_transfers_count=kwargs.get("pending_transfers_count", an_integer()),
        failed_transfers_count=kwargs.get("failed_transfers_count", an_integer()),
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
    national_metrics = _build_national_metrics()
    actual = construct_national_metrics(national_metrics, a_year, a_month)

    assert actual.generated_on == expected_generated_on


def test_has_transfer_count_of_all_transfers():
    expected_initiated_transfers_count = an_integer(2, 7)
    national_metrics = _build_national_metrics(
        initiated_transfers_count=expected_initiated_transfers_count
    )
    actual = construct_national_metrics(national_metrics, a_year, a_month)

    assert actual.metrics[0].transfer_count == expected_initiated_transfers_count


def test_has_integrated_transfer_count():
    expected_integrated_transfer_count = an_integer(2, 7)
    national_metrics = _build_national_metrics(
        integrated_transfer_count=expected_integrated_transfer_count
    )
    actual = construct_national_metrics(national_metrics, a_year, a_month)

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
    national_metrics = _build_national_metrics(
        within_3_days=national_metrics_integrated["within_3_days"],
        within_8_days=national_metrics_integrated["within_8_days"],
        beyond_8_days=national_metrics_integrated["beyond_8_days"],
    )
    actual_integrated_metrics = (
        construct_national_metrics(national_metrics, a_year, a_month).metrics[0].integrated
    )

    assert actual_integrated_metrics.within_3_days == national_metrics_integrated["within_3_days"]
    assert actual_integrated_metrics.within_8_days == national_metrics_integrated["within_8_days"]
    assert actual_integrated_metrics.beyond_8_days == national_metrics_integrated["beyond_8_days"]


def test_has_integrated_percentage():
    initiated_transfers_count = 3
    national_metrics = _build_national_metrics(
        initiated_transfers_count=initiated_transfers_count, integrated_transfer_count=1
    )
    expected_percentage = 33.33
    actual = construct_national_metrics(national_metrics, a_year, a_month)

    assert actual.metrics[0].integrated.transfer_percentage == expected_percentage


def test_has_paper_fallback_transfer_count():
    initiated_transfers_count = 10
    national_metrics = _build_national_metrics(
        initiated_transfers_count=initiated_transfers_count,
        within_3_days=5,
        within_8_days=2,
        beyond_8_days=1,
    )
    actual = construct_national_metrics(national_metrics, a_year, a_month)
    expected = 3

    assert actual.metrics[0].paper_fallback.transfer_count == expected


def test_has_paper_fallback_transfer_percentage():
    transfer_count = 18
    national_metrics = _build_national_metrics(
        transfer_count=transfer_count, within_3_days=10, within_8_days=5, beyond_8_days=3
    )

    expected_percentage = 16.67
    actual = construct_national_metrics(national_metrics, a_year, a_month)

    assert actual.metrics[0].paper_fallback.transfer_percentage == expected_percentage


def test_has_2021_year_and_jan_month():
    expected_year = 2021
    expected_month = 1

    national_metrics = _build_national_metrics()

    actual = construct_national_metrics(national_metrics, expected_year, expected_month)

    assert actual.metrics[0].year == expected_year
    assert actual.metrics[0].month == expected_month
