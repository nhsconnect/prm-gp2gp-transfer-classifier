from collections import defaultdict
from typing import Iterator, Dict, List

from gp2gp.models.spine import Conversation, Message


def transform_messages_to_conversations(messages: Iterator[Message]) -> Iterator[Conversation]:
    conversations: Dict[str, List[Message]] = defaultdict(list)

    for message in messages:
        conversations[message.conversation_id].append(message)

    return (
        Conversation(conversation_id, sorted(messages, key=lambda m: m.time))
        for conversation_id, messages in conversations.items()
    )
