from gp2gp.models.gp2gp import Transfer
from gp2gp.models.spine import ParsedConversation


def build_transfer(conversation: ParsedConversation) -> Transfer:
    return Transfer(conversation_id=conversation.id)
