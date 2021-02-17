from gp2gp.service.nationalMetricsByMonth import calculate_national_metrics_by_month
from tests.builders.service import build_transfers
from tests.builders.common import an_integer


def test_returns_total_transfers_count():
    total_count = an_integer(2, 10)
    transfers = build_transfers(total_count)
    national_metrics = calculate_national_metrics_by_month(transfers)
    assert national_metrics.total_count == total_count


def test_returns_integrated_transfers_count_zero_by_default():
    total_count = an_integer(2, 10)
    transfers = build_transfers(total_count)
    national_metrics = calculate_national_metrics_by_month(transfers)
    assert national_metrics.integrated.total_count == 0


def test_returns_total_integrated_transfers_count():
    total_count = an_integer(7, 10)
    successful_transfers_count = an_integer(2, 4)
    transfers = build_transfers(
        total_count=total_count, successful_transfers_count=successful_transfers_count
    )
    national_metrics = calculate_national_metrics_by_month(transfers)
    assert national_metrics.integrated.total_count == successful_transfers_count
