from dataclasses import dataclass
from typing import Iterable, List, Set
from prmdata.domain.gp2gp.sla import SlaCounter
from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    TransferFailureReason,
)

_PENDING_TRANSFER_REASONS = {
    (
        TransferStatus.PROCESS_FAILURE,
        TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
    ),
    (
        TransferStatus.TECHNICAL_FAILURE,
        TransferFailureReason.REQUEST_NOT_ACKNOWLEDGED,
    ),
    (
        TransferStatus.TECHNICAL_FAILURE,
        TransferFailureReason.CORE_EHR_NOT_SENT,
    ),
    (
        TransferStatus.TECHNICAL_FAILURE,
        TransferFailureReason.COPC_NOT_SENT,
    ),
    (
        TransferStatus.TECHNICAL_FAILURE,
        TransferFailureReason.COPC_NOT_SENT,
    ),
    (
        TransferStatus.TECHNICAL_FAILURE,
        TransferFailureReason.COPC_NOT_ACKNOWLEDGED,
    ),
    (
        TransferStatus.TECHNICAL_FAILURE,
        TransferFailureReason.FATAL_SENDER_ERROR,
    ),
    (
        TransferStatus.UNCLASSIFIED_FAILURE,
        TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR,
    ),
}


@dataclass
class IntegratedMetrics:
    transfer_count: int
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


@dataclass
class NationalMetrics:
    initiated_transfer_count: int
    pending_transfer_count: int
    failed_transfer_count: int
    integrated: IntegratedMetrics

    def calculate_paper_fallback(self):
        integrated_within_sla = self.integrated.within_3_days + self.integrated.within_8_days
        return self.initiated_transfer_count - integrated_within_sla


def calculate_national_metrics(transfers: List[Transfer]) -> NationalMetrics:
    integrated_transfers = _filter_for_integrated_transfers(transfers)
    sla_band_counts = _calculate_sla_band_counts(integrated_transfers)

    return NationalMetrics(
        initiated_transfer_count=len(transfers),
        pending_transfer_count=_count_pending_transfers(transfers),
        failed_transfer_count=_count_failed_transfers(transfers),
        integrated=IntegratedMetrics(
            transfer_count=len(integrated_transfers),
            within_3_days=sla_band_counts.within_3_days(),
            within_8_days=sla_band_counts.within_8_days(),
            beyond_8_days=sla_band_counts.beyond_8_days(),
        ),
    )


def _count_failed_transfers(transfers: Iterable[Transfer]) -> int:
    return len(
        [t for t in transfers if t.outcome.failure_reason == TransferFailureReason.FINAL_ERROR]
    )


def _count_transfers_with_statuses(
    transfers: Iterable[Transfer], statuses: Set[TransferStatus]
) -> int:
    return len([t for t in transfers if t.outcome.status in statuses])


def _count_pending_transfers(transfers: Iterable[Transfer]):
    count = 0
    for transfer in transfers:
        outcome = transfer.outcome
        if (outcome.status, outcome.failure_reason) in _PENDING_TRANSFER_REASONS:
            count += 1
    return count


def _filter_for_integrated_transfers(transfers: Iterable[Transfer]) -> List[Transfer]:
    return [
        t
        for t in transfers
        if (
            (t.outcome.status == TransferStatus.INTEGRATED_ON_TIME)
            or (
                (t.outcome.status == TransferStatus.PROCESS_FAILURE)
                and (t.outcome.failure_reason == TransferFailureReason.INTEGRATED_LATE)
            )
        )
    ]


def _calculate_sla_band_counts(integrated_transfers: List[Transfer]):
    counter = SlaCounter()
    for transfer in integrated_transfers:
        counter.increment(transfer.sla_duration)
    return counter
