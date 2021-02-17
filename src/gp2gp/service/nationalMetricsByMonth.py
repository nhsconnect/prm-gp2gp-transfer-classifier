from typing import NamedTuple, Iterable
from gp2gp.service.transfer import Transfer, TransferStatus


class IntegratedMetrics(NamedTuple):
    total_count: int


class NationalMetricsByMonth(NamedTuple):
    total_count: int
    integrated: IntegratedMetrics


def calculate_national_metrics_by_month(transfers: Iterable[Transfer]) -> NationalMetricsByMonth:
    total_count = 0
    total_integrated_count = 0

    for transfer in transfers:
        total_count += 1
        if transfer.status == TransferStatus.INTEGRATED:
            total_integrated_count += 1

    return NationalMetricsByMonth(
        total_count=total_count, integrated=IntegratedMetrics(total_count=total_integrated_count)
    )
