from dataclasses import dataclass
from typing import Iterable, Tuple, Dict
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


def calculate_national_metrics(transfers: Iterable[Transfer]) -> NationalMetrics:

    integrated_transfer_count, transfers_initiated_count, sla_count = _calculate_transfer_count(
        transfers
    )

    return _create_national_metrics(
        transfers_initiated_count,
        integrated_transfer_count,
        sla_count,
    )


def _create_national_metrics(
    transfers_initiated_count: int,
    integrated_transfer_count: int,
    sla_count: Dict[SlaBand, int],
):
    return NationalMetrics(
        transfers_initiated_count=transfers_initiated_count,
        integrated=IntegratedMetrics(
            transfer_count=integrated_transfer_count,
            within_3_days=sla_count[SlaBand.WITHIN_3_DAYS],
            within_8_days=sla_count[SlaBand.WITHIN_8_DAYS],
            beyond_8_days=sla_count[SlaBand.BEYOND_8_DAYS],
        ),
    )


def _calculate_transfer_count(transfers: Iterable[Transfer]) -> Tuple[int, int, Dict[SlaBand, int]]:
    sla_count = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}
    integrated_transfer_count = 0
    transfers_initiated_count = 0

    for transfer in transfers:
        transfers_initiated_count += 1
        integrated_transfer_count = _calculate_integrated_transfer_count(
            integrated_transfer_count, sla_count, transfer
        )
    return integrated_transfer_count, transfers_initiated_count, sla_count


def _calculate_integrated_transfer_count(
    integrated_transfer_count: int, sla_count: Dict[SlaBand, int], transfer: Transfer
) -> int:
    if transfer.status == TransferStatus.INTEGRATED:
        integrated_transfer_count += 1
        sla_band = assign_to_sla_band(transfer.sla_duration)
        sla_count[sla_band] += 1
    return integrated_transfer_count
