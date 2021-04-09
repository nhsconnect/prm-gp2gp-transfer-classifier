from typing import NamedTuple, List, Optional, Iterable, Iterator

from prmdata.domain.spine.conversation import Conversation
from prmdata.domain.spine.message import Message
from prmdata.utils.date.range import DateTimeRange

EHR_REQUEST_STARTED = "urn:nhs:names:services:gp2gp/RCMR_IN010000UK05"
EHR_REQUEST_COMPLETED = "urn:nhs:names:services:gp2gp/RCMR_IN030000UK06"
APPLICATION_ACK = "urn:nhs:names:services:gp2gp/MCCI_IN010000UK13"
COMMON_POINT_TO_POINT = "urn:nhs:names:services:gp2gp/COPC_IN000001UK01"


class ParsedConversation(NamedTuple):
    id: str
    request_started: Message
    request_started_ack: Optional[Message]
    request_completed: Optional[Message]
    intermediate_messages: List[Message]
    request_completed_ack: Optional[Message]
    request_completed_messages: List[Message]
    request_completed_ack_codes: List[int]


def parse_conversation(conversation: Conversation) -> ParsedConversation:
    parser = SpineConversationParser(conversation)
    return parser.parse()


class ConversationMissingStart(Exception):
    pass


class SpineConversationParser:
    def __init__(self, conversation: Conversation):
        self._id = conversation.id
        self._messages = iter(conversation.messages)
        self._req_started_message: Optional[Message] = None
        self._req_completed_message: Optional[Message] = None
        self._request_started_ack: Optional[Message] = None
        self._intermediate_messages: List[Message] = []
        self._request_completed_ack: Optional[Message] = None
        self._request_completed_messages: List[Message] = []
        self.request_completed_ack_codes: List[int] = []

    @staticmethod
    def _is_request_completed(message):
        return message.interaction_id == EHR_REQUEST_COMPLETED

    @staticmethod
    def _is_acknowledging(acknowledging_message, acknowledged_message):
        if acknowledged_message is None:
            return False
        else:
            is_ack = acknowledging_message.interaction_id == APPLICATION_ACK
            is_acknowledging_candidate_message = (
                acknowledging_message.message_ref == acknowledged_message.guid
            )
            return is_ack and is_acknowledging_candidate_message

    def _get_next_or_none(self):
        next_message = next(self._messages, None)
        return next_message

    def _process_message(self, message):
        if self._is_request_completed(message):
            self._req_completed_message = message
            self._request_completed_messages.append(message)
        elif self._is_acknowledging(message, self._req_completed_message):
            self._request_completed_ack = message
            self.request_completed_ack_codes.append(message.error_code)
        elif self._is_acknowledging_any_request_completed_message(message):
            self.request_completed_ack_codes.append(message.error_code)
        elif self._is_acknowledging(message, self._req_started_message):
            self._request_started_ack = message
        else:
            self._intermediate_messages.append(message)

    def _is_acknowledging_any_request_completed_message(self, message):
        for req_completed in self._request_completed_messages:
            if self._is_acknowledging(message, req_completed):
                return True
        return False

    def parse(self):
        self._req_started_message = self._get_next_or_none()

        if self._req_started_message.interaction_id != EHR_REQUEST_STARTED:
            raise ConversationMissingStart()

        next_message = self._get_next_or_none()
        while next_message is not None:
            self._process_message(next_message)
            next_message = self._get_next_or_none()

        return ParsedConversation(
            self._id,
            request_started=self._req_started_message,
            request_started_ack=self._request_started_ack,
            request_completed=self._req_completed_message,
            intermediate_messages=self._intermediate_messages,
            request_completed_ack=self._request_completed_ack,
            request_completed_messages=self._request_completed_messages,
            request_completed_ack_codes=self.request_completed_ack_codes,
        )


def filter_conversations_by_request_started_time(
    conversations: Iterable[ParsedConversation], time_range: DateTimeRange
) -> Iterator[ParsedConversation]:
    return (
        conversation
        for conversation in conversations
        if time_range.contains(conversation.request_started.time)
    )
