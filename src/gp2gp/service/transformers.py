from datetime import timedelta
from typing import Iterable, Iterator, List
from warnings import warn

from gp2gp.odsportal.models import OrganisationDetails
from gp2gp.service.models import (
    Transfer,
    TransferStatus,
    ERROR_SUPPRESSED,
    PracticeSlaMetrics,
    SlaBand,
    SuccessfulTransfer,
)
from gp2gp.spine.models import ParsedConversation, Message


THREE_DAYS_IN_SECONDS = 259200
EIGHT_DAYS_IN_SECONDS = 691200


def _calculate_sla(conversation):
    if conversation.request_completed is None or conversation.request_completed_ack is None:
        return None
    return conversation.request_completed_ack.time - conversation.request_completed.time


def _extract_requesting_practice_ods_code(conversation):
    return conversation.request_started.from_party_ods_code


def _extract_sending_practice_ods_code(conversation):
    return conversation.request_started.to_party_ods_code


def _extract_final_error_code(conversation):
    if conversation.request_completed_ack:
        return conversation.request_completed_ack.error_code
    return None


def _extract_intermediate_error_code(intermediate_messages: List[Message]):
    return [
        message.error_code for message in intermediate_messages if message.error_code is not None
    ]


def _is_pending(conversation):
    return conversation.request_completed_ack is None


def _derive_transfer(conversation: ParsedConversation) -> Transfer:
    return Transfer(
        conversation_id=conversation.id,
        sla_duration=_calculate_sla(conversation),
        requesting_practice_ods_code=_extract_requesting_practice_ods_code(conversation),
        sending_practice_ods_code=_extract_sending_practice_ods_code(conversation),
        final_error_code=_extract_final_error_code(conversation),
        intermediate_error_codes=_extract_intermediate_error_code(
            conversation.intermediate_messages
        ),
        pending=_is_pending(conversation),
        status=_assign_status(conversation),
    )


def _assign_status(conversation: ParsedConversation) -> TransferStatus:
    if _is_integrated(conversation):
        return TransferStatus.INTEGRATED
    elif _has_error(conversation):
        return TransferStatus.FAILED
    else:
        return TransferStatus.REQUESTED


def _is_integrated(conversation: ParsedConversation) -> bool:
    final_ack = conversation.request_completed_ack
    return final_ack and (final_ack.error_code is None or final_ack.error_code == ERROR_SUPPRESSED)


def _has_error(conversation: ParsedConversation) -> bool:
    return _has_final_ack_error(conversation) or _has_intermediate_message_error(conversation)


def _has_final_ack_error(conversation: ParsedConversation) -> bool:
    final_ack = conversation.request_completed_ack
    return final_ack and final_ack.error_code and final_ack.error_code != ERROR_SUPPRESSED


def _has_intermediate_message_error(conversation: ParsedConversation) -> bool:
    intermediate_errors = _extract_intermediate_error_code(conversation.intermediate_messages)
    return conversation.request_completed_ack is None and len(intermediate_errors) > 0


def derive_transfers(conversations: Iterable[ParsedConversation]) -> Iterator[Transfer]:
    return (_derive_transfer(c) for c in conversations)


def is_successful(transfer):
    return transfer.final_error_code is None or transfer.final_error_code == ERROR_SUPPRESSED


def filter_for_successful_transfers(transfers: Iterable[Transfer]) -> Iterator[SuccessfulTransfer]:
    return (
        SuccessfulTransfer(
            conversation_id=t.conversation_id,
            sla_duration=t.sla_duration,
            requesting_practice_ods_code=t.requesting_practice_ods_code,
            sending_practice_ods_code=t.sending_practice_ods_code,
        )
        for t in transfers
        if is_successful(t) and not t.pending and t.sla_duration is not None
    )


def _assign_to_sla_band(sla_duration: timedelta):
    sla_duration_in_seconds = sla_duration.total_seconds()
    if sla_duration_in_seconds <= THREE_DAYS_IN_SECONDS:
        return SlaBand.WITHIN_3_DAYS
    elif sla_duration_in_seconds <= EIGHT_DAYS_IN_SECONDS:
        return SlaBand.WITHIN_8_DAYS
    else:
        return SlaBand.BEYOND_8_DAYS


def calculate_sla_by_practice(
    practice_list: Iterable[OrganisationDetails], transfers: Iterable[SuccessfulTransfer]
) -> Iterator[PracticeSlaMetrics]:

    default_sla = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}
    practice_counts = {p.ods_code: default_sla.copy() for p in practice_list}

    for transfer in transfers:
        ods_code = transfer.requesting_practice_ods_code
        if ods_code in practice_counts:
            sla_band = _assign_to_sla_band(transfer.sla_duration)
            practice_counts[ods_code][sla_band] += 1
        else:
            warn(f"Unexpected ODS code found: {ods_code}", RuntimeWarning)

    return (
        PracticeSlaMetrics(
            p.ods_code,
            p.name,
            within_3_days=practice_counts[p.ods_code][SlaBand.WITHIN_3_DAYS],
            within_8_days=practice_counts[p.ods_code][SlaBand.WITHIN_8_DAYS],
            beyond_8_days=practice_counts[p.ods_code][SlaBand.BEYOND_8_DAYS],
        )
        for p in practice_list
    )
