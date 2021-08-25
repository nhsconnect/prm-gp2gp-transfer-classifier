from dataclasses import dataclass
from warnings import warn
from datetime import timedelta, datetime
from typing import NamedTuple, Optional, List, Iterator
from enum import Enum

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation


class TransferStatus(Enum):
    INTEGRATED_ON_TIME = "INTEGRATED_ON_TIME"
    TECHNICAL_FAILURE = "TECHNICAL_FAILURE"
    PROCESS_FAILURE = "PROCESS_FAILURE"
    UNCLASSIFIED_FAILURE = "UNCLASSIFIED_FAILURE"


class TransferFailureReason(Enum):
    INTEGRATED_LATE = "Integrated Late"
    FINAL_ERROR = "Final Error"
    TRANSFERRED_NOT_INTEGRATED = "Transferred, not integrated"
    REQUEST_NOT_ACKNOWLEDGED = "Request not Acknowledged"
    CORE_EHR_NOT_SENT = "Core Extract not Sent"
    FATAL_SENDER_ERROR = "Contains Fatal Sender Error"
    COPC_NOT_SENT = "COPC(s) not sent"
    COPC_NOT_ACKNOWLEDGED = "COPC(s) not Acknowledged"
    TRANSFERRED_NOT_INTEGRATED_WITH_ERROR = "Transferred, not integrated, with error"
    AMBIGUOUS_COPCS = "Ambiguous COPC messages"


@dataclass
class TransferOutcome:
    status: TransferStatus
    failure_reason: Optional[TransferFailureReason]


@dataclass
class Practice:
    asid: str
    supplier: str


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice: Practice
    sending_practice: Practice
    sender_error_codes: List[Optional[int]]
    final_error_codes: List[Optional[int]]
    intermediate_error_codes: List[int]
    outcome: TransferOutcome
    date_requested: datetime
    date_completed: Optional[datetime]

    @property
    def sla_duration_seconds(self) -> Optional[int]:
        if self.sla_duration is not None:
            return round(self.sla_duration.total_seconds())
        else:
            return None

    @property
    def requesting_practice_asid(self) -> str:
        return self.requesting_practice.asid

    @property
    def requesting_supplier(self) -> str:
        return self.requesting_practice.supplier

    @property
    def sending_practice_asid(self) -> str:
        return self.sending_practice.asid

    @property
    def sending_supplier(self) -> str:
        return self.sending_practice.supplier

    @property
    def status_description(self) -> str:
        return self.outcome.status.value

    @property
    def failure_reason(self) -> Optional[str]:
        failure_reason = self.outcome.failure_reason
        return None if failure_reason is None else failure_reason.value


def _calculate_sla(conversation: Gp2gpConversation) -> Optional[timedelta]:
    final_acknowledgement_time = conversation.effective_final_acknowledgement_time()
    request_completed_time = conversation.effective_request_completed_time()

    if final_acknowledgement_time is None:
        return None

    sla_duration = final_acknowledgement_time - request_completed_time

    if sla_duration.total_seconds() < 0:
        warn(
            f"Negative SLA duration for conversation: {conversation.conversation_id()}",
            RuntimeWarning,
        )

    return max(timedelta(0), sla_duration)


def _copc_transfer_outcome(conversation: Gp2gpConversation) -> TransferOutcome:
    if conversation.contains_unacknowledged_duplicate_ehr_and_copc_fragments():
        return _unclassified_failure(TransferFailureReason.AMBIGUOUS_COPCS)
    elif conversation.contains_copc_error() and not conversation.is_missing_copc_ack():
        return _unclassified_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR)
    elif conversation.is_missing_copc():
        return _technical_failure(TransferFailureReason.COPC_NOT_SENT)
    elif conversation.is_missing_copc_ack():
        return _technical_failure(TransferFailureReason.COPC_NOT_ACKNOWLEDGED)
    else:
        return _process_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED)


# flake8: noqa: C901
def _assign_transfer_outcome(conversation: Gp2gpConversation) -> TransferOutcome:
    if conversation.is_integrated():
        return _integrated_within_sla(conversation)
    elif conversation.has_concluded_with_failure():
        return _technical_failure(TransferFailureReason.FINAL_ERROR)
    elif conversation.contains_copc_fragments():
        return _copc_transfer_outcome(conversation)
    elif conversation.contains_fatal_sender_error_code():
        return _technical_failure(TransferFailureReason.FATAL_SENDER_ERROR)
    elif conversation.is_missing_request_acknowledged():
        return _technical_failure(TransferFailureReason.REQUEST_NOT_ACKNOWLEDGED)
    elif conversation.is_missing_core_ehr():
        return _technical_failure(TransferFailureReason.CORE_EHR_NOT_SENT)
    elif conversation.contains_core_ehr_with_sender_error():
        return _unclassified_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR)
    else:
        return _process_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED)


def _integrated_within_sla(conversation: Gp2gpConversation) -> TransferOutcome:
    sla_duration = _calculate_sla(conversation)
    if sla_duration is not None and sla_duration <= timedelta(days=8):
        return _integrated_on_time()
    return _process_failure(TransferFailureReason.INTEGRATED_LATE)


def derive_transfer(conversation: Gp2gpConversation) -> Transfer:
    return Transfer(
        conversation_id=conversation.conversation_id(),
        sla_duration=_calculate_sla(conversation),
        requesting_practice=Practice(
            asid=conversation.requesting_practice_asid(),
            supplier=conversation.requesting_supplier(),
        ),
        sending_practice=Practice(
            asid=conversation.sending_practice_asid(), supplier=conversation.sending_supplier()
        ),
        sender_error_codes=conversation.sender_error_codes(),
        final_error_codes=conversation.final_error_codes(),
        intermediate_error_codes=conversation.intermediate_error_codes(),
        outcome=_assign_transfer_outcome(conversation),
        date_requested=conversation.date_requested(),
        date_completed=conversation.effective_final_acknowledgement_time(),
    )


def filter_for_successful_transfers(transfers: List[Transfer]) -> Iterator[Transfer]:
    return (
        transfer
        for transfer in transfers
        if (
            transfer.outcome.status == TransferStatus.INTEGRATED_ON_TIME
            and transfer.sla_duration is not None
        )
        or (
            transfer.outcome.status == TransferStatus.PROCESS_FAILURE
            and transfer.outcome.failure_reason == TransferFailureReason.INTEGRATED_LATE
        )
    )


def _integrated_on_time() -> TransferOutcome:
    return TransferOutcome(status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None)


def _technical_failure(reason: TransferFailureReason) -> TransferOutcome:
    return TransferOutcome(status=TransferStatus.TECHNICAL_FAILURE, failure_reason=reason)


def _process_failure(reason: TransferFailureReason) -> TransferOutcome:
    return TransferOutcome(status=TransferStatus.PROCESS_FAILURE, failure_reason=reason)


def _unclassified_failure(reason: TransferFailureReason = None) -> TransferOutcome:
    return TransferOutcome(
        status=TransferStatus.UNCLASSIFIED_FAILURE,
        failure_reason=reason,
    )
