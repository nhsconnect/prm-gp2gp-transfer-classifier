from collections import defaultdict
from datetime import datetime
from typing import Iterator, Dict, List, Optional, Iterable

from prmdata.spine.models import (
    Message,
    Conversation,
    ParsedConversation,
    APPLICATION_ACK,
    EHR_REQUEST_STARTED,
    EHR_REQUEST_COMPLETED,
)


def group_into_conversations(messages: Iterable[Message]) -> Iterator[Conversation]:
    conversations: Dict[str, List[Message]] = defaultdict(list)

    for message in messages:
        conversations[message.conversation_id].append(message)

    return (
        Conversation(conversation_id, sorted(messages, key=lambda m: m.time))
        for conversation_id, messages in conversations.items()
    )


def parse_conversation(conversation: Conversation) -> Optional[ParsedConversation]:
    parser = SpineConversationParser(conversation)
    return parser.parse()


class SpineConversationParser:
    def __init__(self, conversation: Conversation):
        self._id = conversation.id
        self._messages = iter(conversation.messages)

    def _advance_until_interaction(self, interaction_id):
        return self._advance_until(lambda m: m.interaction_id == interaction_id)

    def _advance_until_acknowledgment_of(self, message):
        return self._advance_until(
            lambda m: m.interaction_id == APPLICATION_ACK and m.message_ref == message.guid
        )

    def _advance_until(self, func):
        next_message = self._get_next_or_none()
        while next_message is not None and not func(next_message):
            next_message = self._get_next_or_none()
        return next_message

    def _get_next_or_none(self):
        next_message = next(self._messages, None)
        return next_message

    def parse(self):
        req_started_message = self._get_next_or_none()
        if req_started_message.interaction_id != EHR_REQUEST_STARTED:
            return None
        req_completed_message = self._advance_until_interaction(EHR_REQUEST_COMPLETED)
        final_ack = self._advance_until_acknowledgment_of(req_completed_message)
        return ParsedConversation(
            self._id,
            request_started=req_started_message,
            request_completed=req_completed_message,
            request_completed_ack=final_ack,
        )


def filter_conversations_by_request_started_time(
    conversations: Iterable[ParsedConversation], from_time: datetime, to_time: datetime
) -> Iterator[ParsedConversation]:
    return (
        conversation
        for conversation in conversations
        if from_time <= conversation.request_started.time < to_time
    )
