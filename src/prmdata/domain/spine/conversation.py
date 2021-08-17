from collections import defaultdict
from datetime import timedelta
from typing import NamedTuple, List, Iterable, Iterator, Dict

from prmdata.domain.spine.message import Message


class Conversation(NamedTuple):
    id: str
    messages: List[Message]


def _ignore_messages_sent_after(cutoff: timedelta, messages: List[Message]) -> List[Message]:
    first_message_in_conversation = messages[0]
    start_of_conversation = first_message_in_conversation.time
    return [message for message in messages if message.time - start_of_conversation <= cutoff]


def group_into_conversations(
    message_stream: Iterable[Message], cutoff: timedelta
) -> Iterator[Conversation]:
    conversations: Dict[str, List[Message]] = defaultdict(list)

    for message in message_stream:
        conversations[message.conversation_id].append(message)

    for conversation_id, unordered_messages in conversations.items():
        sorted_messages = sorted(unordered_messages, key=lambda m: m.time)
        filtered_messages = _ignore_messages_sent_after(cutoff, sorted_messages)
        yield Conversation(
            conversation_id,
            messages=filtered_messages,
        )
