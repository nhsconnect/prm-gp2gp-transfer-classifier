from collections import defaultdict
from datetime import timedelta
from typing import NamedTuple, List, Iterable, Iterator, Dict

from prmdata.domain.spine.message import Message


class Conversation(NamedTuple):
    id: str
    messages: List[Message]


def _ignore_messages_sent_after(cutoff):
    def filter_messages_outside_cutoff(messages):
        first_message = messages[0]
        return [message for message in messages if message.time - first_message.time <= cutoff]

    return filter_messages_outside_cutoff


def group_into_conversations(
    message_stream: Iterable[Message], cutoff: timedelta = None
) -> Iterator[Conversation]:
    message_filter = None if cutoff is None else _ignore_messages_sent_after(cutoff)
    conversations: Dict[str, List[Message]] = defaultdict(list)

    for message in message_stream:
        conversations[message.conversation_id].append(message)

    for conversation_id, unordered_messages in conversations.items():
        messages = sorted(unordered_messages, key=lambda m: m.time)
        yield Conversation(
            conversation_id,
            messages=messages if message_filter is None else message_filter(messages),
        )
