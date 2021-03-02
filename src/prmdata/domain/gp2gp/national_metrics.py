from dataclasses import dataclass
from typing import Iterable, List
from prmdata.domain.gp2gp.sla import SlaBand, assign_to_sla_band
from prmdata.domain.gp2gp.transfer import Transfer, TransferStatus


@dataclass
class IntegratedMetrics:
    transfer_count: int
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


@dataclass
class NationalMetrics:
    transfers_initiated_count: int
    integrated: IntegratedMetrics


def calculate_national_metrics(transfers: List[Transfer]) -> NationalMetrics:
    transfers_initiated_count = len(transfers)
    integrated_transfers = _filter_for_integrated_transfers(transfers)
    integrated_transfer_count = len(integrated_transfers)
    sla_band_counts = _calculate_sla_band_counts(integrated_transfers)

    return NationalMetrics(
        transfers_initiated_count=transfers_initiated_count,
        integrated=IntegratedMetrics(
            transfer_count=integrated_transfer_count,
            within_3_days=sla_band_counts[SlaBand.WITHIN_3_DAYS],
            within_8_days=sla_band_counts[SlaBand.WITHIN_8_DAYS],
            beyond_8_days=sla_band_counts[SlaBand.BEYOND_8_DAYS],
        ),
    )


def _filter_for_integrated_transfers(transfers: Iterable[Transfer]) -> List[Transfer]:
    return [t for t in transfers if t.status == TransferStatus.INTEGRATED]


def _calculate_sla_band_counts(integrated_transfers: List[Transfer]):
    sla_band_counts = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}
    for transfer in integrated_transfers:
        sla_band = assign_to_sla_band(transfer.sla_duration)
        sla_band_counts[sla_band] += 1
    return sla_band_counts
