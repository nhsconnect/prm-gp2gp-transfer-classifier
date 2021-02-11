from datetime import timedelta, datetime
from typing import Iterable, Iterator, List, Optional
from warnings import warn

from gp2gp.io.dictionary import camelize_dict
from gp2gp.odsportal.models import PracticeDetails
from gp2gp.service.models import (
    Transfer,
    TransferStatus,
    ERROR_SUPPRESSED,
    PracticeSlaMetrics,
    SlaBand,
)
from gp2gp.spine.models import ParsedConversation


THREE_DAYS_IN_SECONDS = 259200
EIGHT_DAYS_IN_SECONDS = 691200


def _calculate_sla(conversation: ParsedConversation):
    if conversation.request_completed is None or conversation.request_completed_ack is None:
        return None
    return conversation.request_completed_ack.time - conversation.request_completed.time


def _extract_requesting_practice_asid(conversation: ParsedConversation) -> str:
    return conversation.request_started.from_party_asid


def _extract_sending_practice_asid(conversation: ParsedConversation) -> str:
    return conversation.request_started.to_party_asid


def _extract_final_error_code(conversation: ParsedConversation) -> Optional[int]:
    if conversation.request_completed_ack:
        return conversation.request_completed_ack.error_code
    return None


def _extract_intermediate_error_code(conversation: ParsedConversation) -> List[Optional[int]]:
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


def convert_transfers_to_dict(transfers: List[Transfer]) -> List[dict]:
    transfer_dicts = []
    for transfer in transfers:
        transfer_dict = transfer._asdict()

        transfer_dict["sla_duration"] = (
            transfer_dict["sla_duration"].total_seconds()
            if transfer_dict["sla_duration"] is not None
            else None
        )
        transfer_dict["date_requested"] = transfer_dict["date_requested"].isoformat()
        transfer_dict["date_completed"] = (
            transfer_dict["date_completed"].isoformat()
            if transfer_dict["date_completed"] is not None
            else None
        )
        transfer_dict["status"] = transfer_dict["status"].value

        transfer_dicts.append(transfer_dict)

    return camelize_dict(transfer_dicts)


def _assign_to_sla_band(sla_duration: timedelta):
    sla_duration_in_seconds = sla_duration.total_seconds()
    if sla_duration_in_seconds <= THREE_DAYS_IN_SECONDS:
        return SlaBand.WITHIN_3_DAYS
    elif sla_duration_in_seconds <= EIGHT_DAYS_IN_SECONDS:
        return SlaBand.WITHIN_8_DAYS
    else:
        return SlaBand.BEYOND_8_DAYS


def calculate_sla_by_practice(
    practice_list: Iterable[PracticeDetails], transfers: Iterable[Transfer]
) -> Iterator[PracticeSlaMetrics]:

    default_sla = {SlaBand.WITHIN_3_DAYS: 0, SlaBand.WITHIN_8_DAYS: 0, SlaBand.BEYOND_8_DAYS: 0}
    practice_counts = {practice.ods_code: default_sla.copy() for practice in practice_list}

    asid_to_ods_mapping = {
        asid: practice.ods_code for practice in practice_list for asid in practice.asids
    }

    unexpected_asids = set()
    for transfer in transfers:
        asid = transfer.requesting_practice_asid

        if asid in asid_to_ods_mapping:
            ods_code = asid_to_ods_mapping[asid]
            sla_band = _assign_to_sla_band(transfer.sla_duration)
            practice_counts[ods_code][sla_band] += 1
        else:
            unexpected_asids.add(asid)

    if len(unexpected_asids) > 0:
        warn(f"Unexpected ASID count: {len(unexpected_asids)}", RuntimeWarning)

    return (
        PracticeSlaMetrics(
            practice.ods_code,
            practice.name,
            within_3_days=practice_counts[practice.ods_code][SlaBand.WITHIN_3_DAYS],
            within_8_days=practice_counts[practice.ods_code][SlaBand.WITHIN_8_DAYS],
            beyond_8_days=practice_counts[practice.ods_code][SlaBand.BEYOND_8_DAYS],
        )
        for practice in practice_list
    )
