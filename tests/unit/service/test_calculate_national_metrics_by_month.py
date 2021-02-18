import datetime

import pytest

from gp2gp.service.common import EIGHT_DAYS_IN_SECONDS, THREE_DAYS_IN_SECONDS
from gp2gp.service.nationalMetricsByMonth import calculate_national_metrics_by_month
from tests.builders.service import build_transfers
from tests.builders.common import an_integer


def test_returns_total_transfers_count_default_given_no_transfers():
    national_metrics = calculate_national_metrics_by_month([])
    assert national_metrics.transfer_count == 0


def test_returns_total_transfers_count():
    transfer_count = an_integer(2, 10)
    transfers = build_transfers(transfer_count=transfer_count)
    national_metrics = calculate_national_metrics_by_month(transfers)
    assert national_metrics.transfer_count == transfer_count


def test_returns_integrated_transfer_count_defaults_given_no_successful_transfers():
    transfer_count = an_integer(2, 10)
    transfers = build_transfers(transfer_count=transfer_count)
    national_metrics = calculate_national_metrics_by_month(transfers)
    assert national_metrics.integrated.transfer_count == 0
    assert national_metrics.integrated.within_3_days == 0
    assert national_metrics.integrated.within_8_days == 0
    assert national_metrics.integrated.beyond_8_days == 0


def test_returns_total_integrated_transfer_count():
    transfer_count = an_integer(7, 10)
    integrated_transfer_count = an_integer(2, 4)
    transfers = build_transfers(
        transfer_count=transfer_count, integrated_transfer_count=integrated_transfer_count
    )
    national_metrics = calculate_national_metrics_by_month(transfers)
    assert national_metrics.integrated.transfer_count == integrated_transfer_count


@pytest.mark.parametrize(
    "sla_duration, expected",
    [
        (
            datetime.timedelta(seconds=THREE_DAYS_IN_SECONDS - 1),
            {"within_3_days": 2, "within_8_days": 0, "beyond_8_days": 0},
        ),
        (
            datetime.timedelta(seconds=EIGHT_DAYS_IN_SECONDS),
            {"within_3_days": 0, "within_8_days": 2, "beyond_8_days": 0},
        ),
        (
            datetime.timedelta(seconds=EIGHT_DAYS_IN_SECONDS + 1),
            {"within_3_days": 0, "within_8_days": 0, "beyond_8_days": 2},
        ),
    ],
)
def test_returns_integrated_transfer_count_by_sla_duration(sla_duration, expected):
    transfer_count = an_integer(7, 10)
    integrated_transfer_count = 2
    transfers = build_transfers(
        transfer_count=transfer_count,
        integrated_transfer_count=integrated_transfer_count,
        sla_duration=sla_duration,
    )
    national_metrics = calculate_national_metrics_by_month(transfers)
    assert national_metrics.integrated.within_3_days == expected["within_3_days"]
    assert national_metrics.integrated.within_8_days == expected["within_8_days"]
    assert national_metrics.integrated.beyond_8_days == expected["beyond_8_days"]
