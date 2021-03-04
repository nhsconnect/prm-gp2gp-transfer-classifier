from collections import defaultdict
from typing import NamedTuple, List, Iterable, Iterator, Dict

from prmdata.domain.spine.message import Message


class Conversation(NamedTuple):
    id: str
    messages: List[Message]


def group_into_conversations(messages: Iterable[Message]) -> Iterator[Conversation]:
    conversations: Dict[str, List[Message]] = defaultdict(list)

    for message in messages:
        conversations[message.conversation_id].append(message)

    return (
        Conversation(conversation_id, sorted(messages, key=lambda m: m.time))
        for conversation_id, messages in conversations.items()
    )
