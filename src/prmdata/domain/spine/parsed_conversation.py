from typing import NamedTuple, List, Optional, Iterable, Iterator

from prmdata.domain.spine.conversation import Conversation
from prmdata.domain.spine.message import (
    Message,
    EHR_REQUEST_STARTED,
)
from prmdata.utils.date.range import DateTimeRange


class ParsedConversation(NamedTuple):
    id: str
    request_started: Message
    request_started_ack: Optional[Message]
    request_completed: Optional[Message]
    intermediate_messages: List[Message]
    request_completed_ack: Optional[Message]


def parse_conversation(conversation: Conversation) -> ParsedConversation:
    parser = SpineConversationParser(conversation)
    return parser.parse()


class ConversationMissingStart(Exception):
    pass


class SpineConversationParser:
    def __init__(self, conversation: Conversation):
        self._id = conversation.id
        self._messages = iter(conversation.messages)
        self._req_started: Optional[Message] = None
        self._req_completed: Optional[Message] = None
        self._request_started_ack: Optional[Message] = None
        self._intermediate_messages: List[Message] = []
        self._request_completed_ack: Optional[Message] = None

    def _get_next_or_none(self):
        next_message = next(self._messages, None)
        return next_message

    def _has_seen_req_completed(self):
        return self._req_completed is not None

    def _has_seen_req_started(self):
        return self._req_started is not None

    def _process_message(self, message):
        if message.is_ehr_request_completed():
            self._req_completed = message
        elif self._has_seen_req_completed() and message.is_acknowledgement_of(self._req_completed):
            self._request_completed_ack = message
        elif self._has_seen_req_started() and message.is_acknowledgement_of(self._req_started):
            self._request_started_ack = message
        else:
            self._intermediate_messages.append(message)

    def parse(self):
        self._req_started = self._get_next_or_none()

        if self._req_started.interaction_id != EHR_REQUEST_STARTED:
            raise ConversationMissingStart()

        next_message = self._get_next_or_none()
        while next_message is not None:
            self._process_message(next_message)
            next_message = self._get_next_or_none()

        return ParsedConversation(
            self._id,
            request_started=self._req_started,
            request_started_ack=self._request_started_ack,
            request_completed=self._req_completed,
            intermediate_messages=self._intermediate_messages,
            request_completed_ack=self._request_completed_ack,
        )


def filter_conversations_by_request_started_time(
    conversations: Iterable[ParsedConversation], time_range: DateTimeRange
) -> Iterator[ParsedConversation]:
    return (
        conversation
        for conversation in conversations
        if time_range.contains(conversation.request_started.time)
    )
