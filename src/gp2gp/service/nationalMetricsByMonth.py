from typing import NamedTuple, Iterable

from gp2gp.service.common import SlaBand, assign_to_sla_band
from gp2gp.service.transfer import Transfer, TransferStatus


class IntegratedMetrics(NamedTuple):
    total_count: int
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


class NationalMetricsByMonth(NamedTuple):
    total_count: int
    integrated: IntegratedMetrics


def calculate_national_metrics_by_month(transfers: Iterable[Transfer]) -> NationalMetricsByMonth:
    total_count = 0
    total_integrated_count = 0
    sla_count = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}

    for transfer in transfers:
        total_count += 1
        if transfer.status == TransferStatus.INTEGRATED:
            total_integrated_count += 1
            sla_band = assign_to_sla_band(transfer.sla_duration)
            sla_count[sla_band] += 1

    return NationalMetricsByMonth(
        total_count=total_count,
        integrated=IntegratedMetrics(
            total_count=total_integrated_count,
            within_3_days=sla_count[SlaBand.WITHIN_3_DAYS],
            within_8_days=sla_count[SlaBand.WITHIN_8_DAYS],
            beyond_8_days=sla_count[SlaBand.BEYOND_8_DAYS],
        ),
    )
