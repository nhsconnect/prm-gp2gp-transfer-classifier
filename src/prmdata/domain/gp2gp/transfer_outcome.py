from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional

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
    @classmethod
    def from_gp2gp_conversation(
        cls, conversation: Gp2gpConversation, sla_duration: Optional[timedelta]
    ):
        transfer_outcome = _assign_transfer_outcome(conversation, sla_duration)
        return cls(transfer_outcome.status, transfer_outcome.failure_reason)

    def __init__(self, status: TransferStatus, failure_reason: Optional[TransferFailureReason]):
        self.status = status
        self.failure_reason = failure_reason


# flake8: noqa: C901
def _assign_transfer_outcome(
    conversation: Gp2gpConversation, sla_duration: Optional[timedelta]
) -> TransferOutcome:
    if conversation.is_integrated():
        return _integrated_within_sla(sla_duration)
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


def _integrated_within_sla(sla_duration: Optional[timedelta]) -> TransferOutcome:
    if sla_duration is not None and sla_duration <= timedelta(days=8):
        return _integrated_on_time()
    return _process_failure(TransferFailureReason.INTEGRATED_LATE)


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
