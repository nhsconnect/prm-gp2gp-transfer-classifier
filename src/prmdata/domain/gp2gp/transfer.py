from dataclasses import dataclass
from warnings import warn
from datetime import timedelta, datetime
from typing import NamedTuple, Optional, List, Iterable, Iterator
from enum import Enum

import pyarrow as pa
import pyarrow as Table

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

    @property
    def status_string(self):
        return self.status.value

    @property
    def failure_reason_string(self):
        return None if self.failure_reason is None else self.failure_reason.value


@dataclass
class Practice:
    asid: str
    supplier: str


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice: Practice
    sending_practice: Practice
    sender_error_code: Optional[int]
    final_error_codes: List[Optional[int]]
    intermediate_error_codes: List[int]
    outcome: TransferOutcome
    date_requested: datetime
    date_completed: Optional[datetime]


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
    if conversation.contains_unacknowledged_duplicate_ehr_and_copcs():
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
    elif conversation.contains_copc_messages():
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
        sender_error_code=conversation.sender_error(),
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


def _convert_to_seconds(duration: Optional[timedelta]) -> Optional[int]:
    if duration is not None:
        return round(duration.total_seconds())
    else:
        return None


def convert_transfers_to_table(transfers: Iterable[Transfer]) -> Table:
    return pa.table(
        {
            "conversation_id": [t.conversation_id for t in transfers],
            "sla_duration": [_convert_to_seconds(t.sla_duration) for t in transfers],
            "requesting_practice_asid": [t.requesting_practice.asid for t in transfers],
            "sending_practice_asid": [t.sending_practice.asid for t in transfers],
            "requesting_supplier": [t.requesting_practice.supplier for t in transfers],
            "sending_supplier": [t.sending_practice.supplier for t in transfers],
            "sender_error_code": [t.sender_error_code for t in transfers],
            "final_error_codes": [t.final_error_codes for t in transfers],
            "intermediate_error_codes": [t.intermediate_error_codes for t in transfers],
            "status": [t.outcome.status_string for t in transfers],
            "failure_reason": [t.outcome.failure_reason_string for t in transfers],
            "date_requested": [t.date_requested for t in transfers],
            "date_completed": [t.date_completed for t in transfers],
        },
        schema=pa.schema(
            [
                ("conversation_id", pa.string()),
                ("sla_duration", pa.uint64()),
                ("requesting_practice_asid", pa.string()),
                ("sending_practice_asid", pa.string()),
                ("requesting_supplier", pa.string()),
                ("sending_supplier", pa.string()),
                ("sender_error_code", pa.int64()),
                ("final_error_codes", pa.list_(pa.int64())),
                ("intermediate_error_codes", pa.list_(pa.int64())),
                ("status", pa.string()),
                ("failure_reason", pa.string()),
                ("date_requested", pa.timestamp("us")),
                ("date_completed", pa.timestamp("us")),
            ]
        ),
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
