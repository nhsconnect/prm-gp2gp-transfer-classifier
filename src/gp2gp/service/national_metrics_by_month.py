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
    year: int
    month: int


def calculate_national_metrics_by_month(
    transfers: Iterable[Transfer], year: int, month: int
) -> NationalMetricsByMonth:
    transfer_count = 0
    integrated_transfer_count = 0
    sla_count = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}

    integrated_transfer_count, transfer_count = calculate_transfer_count(integrated_transfer_count, sla_count, transfer_count, transfers)

    return createNationalMetrics(integrated_transfer_count, month, sla_count, transfer_count, year)


def createNationalMetrics(integrated_transfer_count, month, sla_count, transfer_count, year):
    return NationalMetricsByMonth(
        year=year,
        month=month,
        transfer_count=transfer_count,
        integrated=IntegratedMetrics(
            transfer_count=integrated_transfer_count,
            within_3_days=sla_count[SlaBand.WITHIN_3_DAYS],
            within_8_days=sla_count[SlaBand.WITHIN_8_DAYS],
            beyond_8_days=sla_count[SlaBand.BEYOND_8_DAYS],
        ),
    )


def calculate_transfer_count(integrated_transfer_count, sla_count, transfer_count, transfers):
    for transfer in transfers:
        transfer_count += 1
        integrated_transfer_count = calculate_integrated_transfer_count(integrated_transfer_count, sla_count, transfer)
    return integrated_transfer_count, transfer_count


def calculate_integrated_transfer_count(integrated_transfer_count, sla_count, transfer):
    if transfer.status == TransferStatus.INTEGRATED:
        integrated_transfer_count += 1
        sla_band = assign_to_sla_band(transfer.sla_duration)
        sla_count[sla_band] += 1
    return integrated_transfer_count
