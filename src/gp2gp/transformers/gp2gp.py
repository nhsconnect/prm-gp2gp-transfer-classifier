from gp2gp.models.gp2gp import Transfer
from gp2gp.models.spine import ParsedConversation


def _calculate_sla(conversation):
    return conversation.request_completed_ack.time - conversation.request_completed.time


def _extract_requesting_practice_ods(conversation):
    return conversation.request_started.from_party_ods


def build_transfer(conversation: ParsedConversation) -> Transfer:
    return Transfer(
        conversation_id=conversation.id,
        sla_duration=_calculate_sla(conversation),
        requesting_practice_ods=_extract_requesting_practice_ods(conversation),
    )
