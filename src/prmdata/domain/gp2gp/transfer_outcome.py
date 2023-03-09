from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional, Tuple

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation


class TransferStatus(Enum):
    INTEGRATED_ON_TIME = "Integrated on time"
    TECHNICAL_FAILURE = "Technical failure"
    PROCESS_FAILURE = "Process failure"
    UNCLASSIFIED_FAILURE = "Unclassified failure"


class TransferFailureReason(Enum):
    INTEGRATED_LATE = "Integrated late"
    FINAL_ERROR = "Final error"
    TRANSFERRED_NOT_INTEGRATED = "Transferred, not integrated"
    REQUEST_NOT_ACKNOWLEDGED = "Request not acknowledged"
    CORE_EHR_NOT_SENT = "Core extract not sent"
    FATAL_SENDER_ERROR = "Contains fatal sender error"
    COPC_NOT_SENT = "COPC(s) not sent"
    COPC_NOT_ACKNOWLEDGED = "COPC(s) not acknowledged"
    TRANSFERRED_NOT_INTEGRATED_WITH_ERROR = "Transferred, not integrated, with error"
    AMBIGUOUS_COPCS = "Ambiguous COPC messages"


@dataclass
class TransferOutcome:
    @classmethod
    def from_gp2gp_conversation(
        cls, conversation: Gp2gpConversation, sla_duration: Optional[timedelta]
    ):
        [status, failure_reason] = _assign_transfer_outcome(conversation, sla_duration)
        return cls(status, failure_reason)

    def __init__(self, status: TransferStatus, failure_reason: Optional[TransferFailureReason]):
        self.status = status
        self.failure_reason = failure_reason


# flake8: noqa: C901
def _assign_transfer_outcome(
    conversation: Gp2gpConversation, sla_duration: Optional[timedelta]
) -> Tuple[TransferStatus, Optional[TransferFailureReason]]:
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


def _copc_transfer_outcome(
    conversation: Gp2gpConversation,
) -> Tuple[TransferStatus, Optional[TransferFailureReason]]:
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


def _integrated_within_sla(
    sla_duration: Optional[timedelta],
) -> Tuple[TransferStatus, Optional[TransferFailureReason]]:
    if sla_duration is not None and sla_duration <= timedelta(days=8):
        return _integrated_on_time()
    return _process_failure(TransferFailureReason.INTEGRATED_LATE)


def _integrated_on_time() -> Tuple[TransferStatus, Optional[TransferFailureReason]]:
    return TransferStatus.INTEGRATED_ON_TIME, None


def _technical_failure(
    reason: TransferFailureReason,
) -> Tuple[TransferStatus, TransferFailureReason]:
    return TransferStatus.TECHNICAL_FAILURE, reason


def _process_failure(reason: TransferFailureReason) -> Tuple[TransferStatus, TransferFailureReason]:
    return TransferStatus.PROCESS_FAILURE, reason


def _unclassified_failure(
    reason: Optional[TransferFailureReason] = None,
) -> Tuple[TransferStatus, Optional[TransferFailureReason]]:
    return TransferStatus.UNCLASSIFIED_FAILURE, reason
