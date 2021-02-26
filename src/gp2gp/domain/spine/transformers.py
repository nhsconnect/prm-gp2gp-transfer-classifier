from collections import defaultdict
from typing import Iterator, Dict, List, Iterable

from gp2gp.utils.date.range import DateTimeRange
from gp2gp.domain.spine.models import (
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


def parse_conversation(conversation: Conversation) -> ParsedConversation:
    parser = SpineConversationParser(conversation)
    return parser.parse()


class ConversationMissingStart(Exception):
    pass


class SpineConversationParser:
    def __init__(self, conversation: Conversation):
        self._id = conversation.id
        self._messages = iter(conversation.messages)

    def _is_request_completed(self, message):
        return message.interaction_id == EHR_REQUEST_COMPLETED

    def _is_final_ack(self, message, req_completed_message):
        if req_completed_message is None:
            return False
        else:
            is_ack = message.interaction_id == APPLICATION_ACK
            is_request_completed_ack = message.message_ref == req_completed_message.guid
            return is_ack and is_request_completed_ack

    def _get_next_or_none(self):
        next_message = next(self._messages, None)
        return next_message

    def parse(self):
        req_started_message = self._get_next_or_none()
        if req_started_message.interaction_id != EHR_REQUEST_STARTED:
            raise ConversationMissingStart()
        req_completed_message = None
        final_ack = None
        intermediate_messages = []

        next_message = self._get_next_or_none()
        while next_message is not None:
            if self._is_request_completed(next_message):
                req_completed_message = next_message
            elif self._is_final_ack(next_message, req_completed_message):
                final_ack = next_message
            else:
                intermediate_messages.append(next_message)
            next_message = self._get_next_or_none()

        return ParsedConversation(
            self._id,
            request_started=req_started_message,
            request_completed=req_completed_message,
            intermediate_messages=intermediate_messages,
            request_completed_ack=final_ack,
        )


def filter_conversations_by_request_started_time(
    conversations: Iterable[ParsedConversation], time_range: DateTimeRange
) -> Iterator[ParsedConversation]:
    return (
        conversation
        for conversation in conversations
        if time_range.contains(conversation.request_started.time)
    )
