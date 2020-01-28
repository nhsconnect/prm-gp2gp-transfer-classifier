from typing import Iterable

from prmdata.gp2gp.models import Transfer, ERROR_SUPPRESSED, PracticeSlaSummary
from prmdata.spine.models import ParsedConversation


def _calculate_sla(conversation):
    if conversation.request_completed is None or conversation.request_completed_ack is None:
        return None
    return conversation.request_completed_ack.time - conversation.request_completed.time


def _extract_requesting_practice_ods(conversation):
    return conversation.request_started.from_party_ods


def _extract_sending_practice_ods(conversation):
    return conversation.request_started.to_party_ods


def _extract_error_code(conversation):
    if conversation.request_completed_ack:
        return conversation.request_completed_ack.error_code
    return None


def _is_pending(conversation):
    return conversation.request_completed_ack is None


def derive_transfer(conversation: ParsedConversation) -> Transfer:
    return Transfer(
        conversation_id=conversation.id,
        sla_duration=_calculate_sla(conversation),
        requesting_practice_ods=_extract_requesting_practice_ods(conversation),
        sending_practice_ods=_extract_sending_practice_ods(conversation),
        error_code=_extract_error_code(conversation),
        pending=_is_pending(conversation),
    )


def _is_successful(transfer):
    return transfer.error_code is None or transfer.error_code == ERROR_SUPPRESSED


def filter_failed_transfers(transfers: Iterable[Transfer]) -> Iterable[Transfer]:
    return (t for t in transfers if _is_successful(t))


def filter_pending_transfers(transfers: Iterable[Transfer]) -> Iterable[Transfer]:
    return (t for t in transfers if not t.pending)


def calculate_sla_by_practice(transfers: Iterable[Transfer]) -> Iterable[PracticeSlaSummary]:
    return iter(
        {
            PracticeSlaSummary(
                ods=transfer.requesting_practice_ods,
                within_3_days=1,
                within_8_days=0,
                beyond_8_days=0,
            )
            for transfer in transfers
        }
    )
