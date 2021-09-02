from collections import defaultdict
from datetime import timedelta
from typing import List, Iterable, Iterator, Dict

from prmdata.domain.gp2gp.transfer import derive_transfer, TransferObservabilityProbe
from prmdata.domain.spine.conversation import Conversation
from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation, ConversationMissingStart
from prmdata.domain.spine.message import Message


class TransferService:
    def __init__(self, message_stream: Iterable[Message], cutoff: timedelta):
        self._probe = TransferObservabilityProbe()
        self._message_stream = message_stream
        self._cutoff = cutoff

    def group_into_conversations(self) -> Iterator[Conversation]:
        conversations: Dict[str, List[Message]] = defaultdict(list)

        for message in self._message_stream:
            conversations[message.conversation_id].append(message)

        for conversation_id, unordered_messages in conversations.items():
            sorted_messages = sorted(unordered_messages, key=lambda m: m.time)
            filtered_messages = _ignore_messages_sent_after(self._cutoff, sorted_messages)
            yield Conversation(
                conversation_id,
                messages=filtered_messages,
            )

    @staticmethod
    def parse_conversations_into_gp2gp_conversations(conversations: Iterator[Conversation]):
        for conversation in conversations:
            try:
                yield Gp2gpConversation(conversation.messages)
            except ConversationMissingStart:
                pass

    def convert_to_transfers(self, conversations: Iterator[Gp2gpConversation]):
        return (derive_transfer(conversation, self._probe) for conversation in conversations)


def _ignore_messages_sent_after(cutoff: timedelta, messages: List[Message]) -> List[Message]:
    first_message_in_conversation = messages[0]
    start_of_conversation = first_message_in_conversation.time
    return [message for message in messages if message.time - start_of_conversation <= cutoff]
