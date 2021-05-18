from warnings import warn
from datetime import timedelta, datetime
from typing import NamedTuple, Optional, List, Iterable, Iterator
from enum import Enum

import pyarrow as pa
import pyarrow as Table

from prmdata.domain.spine.message import Message
from prmdata.domain.spine.parsed_conversation import ParsedConversation

ERROR_SUPPRESSED = 15
DUPLICATE_ERROR = 12


class TransferStatus(Enum):
    INTEGRATED = "INTEGRATED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    PENDING_WITH_ERROR = "PENDING_WITH_ERROR"


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice_asid: str
    sending_practice_asid: str
    requesting_supplier: str
    sending_supplier: str
    sender_error_code: Optional[int]
    final_error_codes: List[Optional[int]]
    intermediate_error_codes: List[int]
    status: TransferStatus
    date_requested: datetime
    date_completed: Optional[datetime]


def _generate_sla(conversation: ParsedConversation):
    successful_acknowledgement = _find_successful_acknowledgement(conversation)
    successful_request_completed_message = _find_acknowledged_request_completed_message(
        conversation, successful_acknowledgement
    )

    if successful_request_completed_message:
        return _calculate_sla(
            successful_acknowledgement,
            successful_request_completed_message,
            conversation.id,
        )

    failed_acknowledgement = _find_failed_acknowledgement(conversation)
    failed_request_completed_message = _find_acknowledged_request_completed_message(
        conversation, failed_acknowledgement
    )

    if failed_request_completed_message:
        return _calculate_sla(
            failed_acknowledgement, failed_request_completed_message, conversation.id
        )
    else:
        return None


def _calculate_sla(acknowledgement_message, request_completed_message, conversation_id):
    sla_duration = acknowledgement_message.time - request_completed_message.time
    if sla_duration.total_seconds() < 0:
        warn(f"Negative SLA duration for conversation: {conversation_id}", RuntimeWarning)
    return max(timedelta(0), sla_duration)


def _find_acknowledged_request_completed_message(
    conversation: ParsedConversation, acknowledgement: Message
) -> Message:
    return next(
        (
            request_completed_message
            for request_completed_message in conversation.request_completed_messages
            if acknowledgement and request_completed_message.guid == acknowledgement.message_ref
        ),
        None,
    )


def _find_successful_acknowledgement(conversation: ParsedConversation) -> Message:
    return next(
        (
            message
            for message in conversation.request_completed_ack_messages
            if _is_successful_ack(message)
        ),
        None,
    )


def _find_failed_acknowledgement(conversation: ParsedConversation) -> Message:
    return next(
        (
            message
            for message in conversation.request_completed_ack_messages
            if not _is_successful_ack(message) and message.error_code != DUPLICATE_ERROR
        ),
        None,
    )


def _extract_date_requested(conversation: ParsedConversation) -> datetime:
    return conversation.request_started.time


def _extract_date_completed(conversation: ParsedConversation) -> Optional[datetime]:
    successful_acknowledgement = _find_successful_acknowledgement(conversation)
    if successful_acknowledgement:
        return successful_acknowledgement.time

    failed_acknowledgement = _find_failed_acknowledgement(conversation)
    if failed_acknowledgement:
        return failed_acknowledgement.time

    return None


def _assign_status(conversation: ParsedConversation) -> TransferStatus:
    if _is_integrated(conversation):
        return TransferStatus.INTEGRATED
    elif _has_final_ack_error(conversation):
        return TransferStatus.FAILED
    elif _has_intermediate_error_and_no_final_ack(conversation):
        return TransferStatus.PENDING_WITH_ERROR
    else:
        return TransferStatus.PENDING


def _is_integrated(conversation: ParsedConversation) -> bool:
    final_ack_messages = conversation.request_completed_ack_messages
    return len(final_ack_messages) > 0 and any(
        _is_successful_ack(final_ack_message) for final_ack_message in final_ack_messages
    )


def _is_successful_ack(message: Message) -> bool:
    return message.error_code is None or message.error_code == ERROR_SUPPRESSED


def _has_final_ack_error(conversation: ParsedConversation) -> bool:
    final_ack_messages = conversation.request_completed_ack_messages
    return len(final_ack_messages) > 0 and any(
        not _is_successful_ack(final_ack_message)
        and final_ack_message.error_code != DUPLICATE_ERROR
        for final_ack_message in final_ack_messages
    )


def _has_intermediate_error_and_no_final_ack(conversation: ParsedConversation) -> bool:
    intermediate_errors = conversation.intermediate_error_codes()
    sender_error = conversation.sender_error()
    has_intermediate_error = len(intermediate_errors) > 0 or sender_error is not None
    lacking_final_ack = len(conversation.request_completed_ack_messages) == 0
    return lacking_final_ack and has_intermediate_error


def _derive_transfer(conversation: ParsedConversation) -> Transfer:
    return Transfer(
        conversation_id=conversation.id,
        sla_duration=_generate_sla(conversation),
        requesting_practice_asid=conversation.requesting_practice_asid(),
        sending_practice_asid=conversation.sending_practice_asid(),
        requesting_supplier=conversation.requesting_supplier(),
        sending_supplier=conversation.sending_supplier(),
        sender_error_code=conversation.sender_error(),
        final_error_codes=conversation.final_error_codes(),
        intermediate_error_codes=conversation.intermediate_error_codes(),
        status=_assign_status(conversation),
        date_requested=_extract_date_requested(conversation),
        date_completed=_extract_date_completed(conversation),
    )


def derive_transfers(conversations: Iterable[ParsedConversation]) -> Iterator[Transfer]:
    return (_derive_transfer(conversation) for conversation in conversations)


def filter_for_successful_transfers(transfers: List[Transfer]) -> Iterator[Transfer]:
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
            "requesting_supplier": [t.requesting_supplier for t in transfers],
            "sending_supplier": [t.sending_supplier for t in transfers],
            "sender_error_code": [t.sender_error_code for t in transfers],
            "final_error_codes": [t.final_error_codes for t in transfers],
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
                ("requesting_supplier", pa.string()),
                ("sending_supplier", pa.string()),
                ("sender_error_code", pa.int64()),
                ("final_error_codes", pa.list_(pa.int64())),
                ("intermediate_error_codes", pa.list_(pa.int64())),
                ("status", pa.string()),
                ("date_requested", pa.timestamp("us")),
                ("date_completed", pa.timestamp("us")),
            ]
        ),
    )
