from dataclasses import dataclass
from typing import Iterable

from gp2gp.service.common import SlaBand, assign_to_sla_band
from gp2gp.service.transfer import Transfer, TransferStatus


@dataclass
class IntegratedMetrics:
    transfer_count: int
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


@dataclass
class NationalMetricsByMonth:
    transfer_count: int
    integrated: IntegratedMetrics


def calculate_national_metrics_by_month(transfers: Iterable[Transfer]) -> NationalMetricsByMonth:
    transfer_count = 0
    integrated_transfer_count = 0
    sla_count = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}

    for transfer in transfers:
        transfer_count += 1
        if transfer.status == TransferStatus.INTEGRATED:
            integrated_transfer_count += 1
            sla_band = assign_to_sla_band(transfer.sla_duration)
            sla_count[sla_band] += 1

    return NationalMetricsByMonth(
        transfer_count=transfer_count,
        integrated=IntegratedMetrics(
            transfer_count=integrated_transfer_count,
            within_3_days=sla_count[SlaBand.WITHIN_3_DAYS],
            within_8_days=sla_count[SlaBand.WITHIN_8_DAYS],
            beyond_8_days=sla_count[SlaBand.BEYOND_8_DAYS],
        ),
    )
