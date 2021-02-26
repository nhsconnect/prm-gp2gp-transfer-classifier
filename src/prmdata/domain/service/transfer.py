from _warnings import warn
from datetime import timedelta, datetime
from typing import NamedTuple, Optional, List, Iterable, Iterator
from enum import Enum

import pyarrow as pa
import pyarrow as Table

from prmdata.domain.spine.models import ParsedConversation

ERROR_SUPPRESSED = 15


class TransferStatus(Enum):
    PENDING = "PENDING"
    INTEGRATED = "INTEGRATED"
    FAILED = "FAILED"


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice_asid: str
    sending_practice_asid: str
    final_error_code: Optional[int]
    intermediate_error_codes: List[int]
    status: TransferStatus
    date_requested: datetime
    date_completed: Optional[datetime]


def _calculate_sla(conversation: ParsedConversation):
    if conversation.request_completed is None or conversation.request_completed_ack is None:
        return None

    sla_duration = conversation.request_completed_ack.time - conversation.request_completed.time
    if sla_duration.total_seconds() < 0:
        warn(f"Negative SLA duration for conversation: {conversation.id}", RuntimeWarning)

    return max(timedelta(0), sla_duration)


def _extract_requesting_practice_asid(conversation: ParsedConversation) -> str:
    return conversation.request_started.from_party_asid


def _extract_sending_practice_asid(conversation: ParsedConversation) -> str:
    return conversation.request_started.to_party_asid


def _extract_final_error_code(conversation: ParsedConversation) -> Optional[int]:
    if conversation.request_completed_ack:
        return conversation.request_completed_ack.error_code
    return None


def _extract_intermediate_error_code(conversation: ParsedConversation) -> List[int]:
    return [
        message.error_code
        for message in conversation.intermediate_messages
        if message.error_code is not None
    ]


def _extract_date_requested(conversation: ParsedConversation) -> datetime:
    return conversation.request_started.time


def _extract_date_completed(conversation: ParsedConversation) -> Optional[datetime]:
    if conversation.request_completed_ack:
        return conversation.request_completed_ack.time
    return None


def _assign_status(conversation: ParsedConversation) -> TransferStatus:
    if _is_integrated(conversation):
        return TransferStatus.INTEGRATED
    elif _has_error(conversation):
        return TransferStatus.FAILED
    else:
        return TransferStatus.PENDING


def _is_integrated(conversation: ParsedConversation) -> bool:
    final_ack = conversation.request_completed_ack
    return final_ack and (final_ack.error_code is None or final_ack.error_code == ERROR_SUPPRESSED)


def _has_error(conversation: ParsedConversation) -> bool:
    return _has_final_ack_error(conversation) or _has_intermediate_message_error(conversation)


def _has_final_ack_error(conversation: ParsedConversation) -> bool:
    final_ack = conversation.request_completed_ack
    return final_ack and final_ack.error_code and final_ack.error_code != ERROR_SUPPRESSED


def _has_intermediate_message_error(conversation: ParsedConversation) -> bool:
    intermediate_errors = _extract_intermediate_error_code(conversation)
    return conversation.request_completed_ack is None and len(intermediate_errors) > 0


def _derive_transfer(conversation: ParsedConversation) -> Transfer:
    return Transfer(
        conversation_id=conversation.id,
        sla_duration=_calculate_sla(conversation),
        requesting_practice_asid=_extract_requesting_practice_asid(conversation),
        sending_practice_asid=_extract_sending_practice_asid(conversation),
        final_error_code=_extract_final_error_code(conversation),
        intermediate_error_codes=_extract_intermediate_error_code(conversation),
        status=_assign_status(conversation),
        date_requested=_extract_date_requested(conversation),
        date_completed=_extract_date_completed(conversation),
    )


def derive_transfers(conversations: Iterable[ParsedConversation]) -> Iterator[Transfer]:
    return (_derive_transfer(conversation) for conversation in conversations)


def filter_for_successful_transfers(transfers: Iterable[Transfer]) -> Iterator[Transfer]:
    return (
        transfer
        for transfer in transfers
        if transfer.status == TransferStatus.INTEGRATED and transfer.sla_duration is not None
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
            "requesting_practice_asid": [t.requesting_practice_asid for t in transfers],
            "sending_practice_asid": [t.sending_practice_asid for t in transfers],
            "final_error_code": [t.final_error_code for t in transfers],
            "intermediate_error_codes": [t.intermediate_error_codes for t in transfers],
            "status": [t.status.value for t in transfers],
            "date_requested": [t.date_requested for t in transfers],
            "date_completed": [t.date_completed for t in transfers],
        },
        schema=pa.schema(
            [
                ("conversation_id", pa.string()),
                ("sla_duration", pa.uint64()),
                ("requesting_practice_asid", pa.string()),
                ("sending_practice_asid", pa.string()),
                ("final_error_code", pa.int64()),
                ("intermediate_error_codes", pa.list_(pa.int64())),
                ("status", pa.string()),
                ("date_requested", pa.timestamp("us")),
                ("date_completed", pa.timestamp("us")),
            ]
        ),
    )