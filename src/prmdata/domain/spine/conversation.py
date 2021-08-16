from collections import defaultdict
from datetime import timedelta
from typing import NamedTuple, List, Iterable, Iterator, Dict

from prmdata.domain.spine.message import Message


class Conversation(NamedTuple):
    id: str
    messages: List[Message]


def _ignore_messages_sent_after(cutoff: timedelta):
    def filter_messages_outside_cutoff(messages: List[Message]) -> List[Message]:
        first_message_in_conversation = messages[0]
        start_of_conversation = first_message_in_conversation.time
        return [message for message in messages if message.time - start_of_conversation <= cutoff]

    return filter_messages_outside_cutoff


def group_into_conversations(
    message_stream: Iterable[Message], cutoff: timedelta
) -> Iterator[Conversation]:
    message_filter = _ignore_messages_sent_after(cutoff)
    conversations: Dict[str, List[Message]] = defaultdict(list)

    for message in message_stream:
        conversations[message.conversation_id].append(message)

    for conversation_id, unordered_messages in conversations.items():
        messages = sorted(unordered_messages, key=lambda m: m.time)
        yield Conversation(
            conversation_id,
            messages=message_filter(messages),
        )
