from typing import Iterable

from gp2gp.models.gp2gp import Transfer
from gp2gp.models.spine import ParsedConversation


def _calculate_sla(conversation):
    return conversation.request_completed_ack.time - conversation.request_completed.time


def _extract_requesting_practice_ods(conversation):
    return conversation.request_started.from_party_ods


def _extract_sending_practice_ods(conversation):
    return conversation.request_started.to_party_ods


def _extract_error_code(conversation):
    return conversation.request_completed_ack.error_code


def derive_transfer(conversation: ParsedConversation) -> Transfer:
    return Transfer(
        conversation_id=conversation.id,
        sla_duration=_calculate_sla(conversation),
        requesting_practice_ods=_extract_requesting_practice_ods(conversation),
        sending_practice_ods=_extract_sending_practice_ods(conversation),
        error_code=_extract_error_code(conversation),
    )


def filter_failed_transfers(transfers: Iterable[Transfer]) -> Iterable[Transfer]:
    return (t for t in transfers if t.error_code is None)
