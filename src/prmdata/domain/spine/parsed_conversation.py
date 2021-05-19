from typing import NamedTuple, List, Optional, Iterable, Iterator
from datetime import datetime

from prmdata.domain.gp2gp.transfer import DUPLICATE_ERROR, ERROR_SUPPRESSED
from prmdata.domain.spine.conversation import Conversation
from prmdata.domain.spine.message import Message
from prmdata.utils.date.range import DateTimeRange


class ParsedConversation(NamedTuple):
    id: str
    request_started: Message
    request_started_ack: Optional[Message]
    request_completed_messages: List[Message]
    intermediate_messages: List[Message]
    request_completed_ack_messages: List[Message]

    def sending_practice_asid(self) -> str:
        return self.request_started.to_party_asid

    def requesting_practice_asid(self) -> str:
        return self.request_started.from_party_asid

    def requesting_supplier(self) -> str:
        return self.request_started.from_system

    def sending_supplier(self) -> str:
        return self.request_started.to_system

    def final_error_codes(self) -> List[Optional[int]]:
        return [message.error_code for message in self.request_completed_ack_messages]

    def sender_error(self) -> Optional[int]:
        if self.request_started_ack:
            return self.request_started_ack.error_code
        return None

    def intermediate_error_codes(self) -> List[int]:
        return [
            message.error_code
            for message in self.intermediate_messages
            if message.error_code is not None
        ]

    def date_requested(self) -> datetime:
        return self.request_started.time

    def _find_successful_request_completed_ack_message(self) -> Optional[Message]:
        return next(
            (
                message
                for message in self.request_completed_ack_messages
                if message.error_code is None or message.error_code == ERROR_SUPPRESSED
            ),
            None,
        )

    def _find_failed_request_completed_ack_message(self) -> Optional[Message]:
        return next(
            (
                message
                for message in self.request_completed_ack_messages
                if message.error_code is not None and message.error_code != DUPLICATE_ERROR
            ),
            None,
        )

    def _find_effective_request_completed_ack_message(self) -> Optional[Message]:
        return (
            self._find_successful_request_completed_ack_message()
            or self._find_failed_request_completed_ack_message()
        )

    def _find_request_completed_by_guid(self, guid: str) -> Optional[Message]:
        return next(
            (message for message in self.request_completed_messages if message.guid == guid), None
        )

    def effective_request_completed_time(self) -> Optional[datetime]:
        effective_request_completed_ack_message = (
            self._find_effective_request_completed_ack_message()
        )

        if effective_request_completed_ack_message is None:
            return None

        effective_request_completed_message = self._find_request_completed_by_guid(
            guid=effective_request_completed_ack_message.message_ref
        )
        if effective_request_completed_message:
            return effective_request_completed_message.time

        return None

    def effective_final_acknowledgement_time(self):
        final_ack = self._find_effective_request_completed_ack_message()
        if final_ack is None:
            return None
        return final_ack.time

    def date_completed(self) -> Optional[datetime]:
        successful_acknowledgement = _find_successful_acknowledgement(self)
        if successful_acknowledgement:
            return successful_acknowledgement.time

        failed_acknowledgement = _find_failed_acknowledgement(self)
        if failed_acknowledgement:
            return failed_acknowledgement.time

        return None


def _find_successful_acknowledgement(conversation: ParsedConversation) -> Message:
    return next(
        (
            message
            for message in conversation.request_completed_ack_messages
            if _is_successful_ack(message)
        ),
        None,
    )


def _find_failed_acknowledgement(conversation: ParsedConversation) -> Message:
    return next(
        (
            message
            for message in conversation.request_completed_ack_messages
            if not _is_successful_ack(message) and message.error_code != DUPLICATE_ERROR
        ),
        None,
    )


def _is_successful_ack(message: Message) -> bool:
    return message.error_code is None or message.error_code == ERROR_SUPPRESSED


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
        self._req_completed_messages: List[Message] = []
        self._request_started_ack: Optional[Message] = None
        self._intermediate_messages: List[Message] = []
        self._request_completed_ack_messages: List[Message] = []

    def _get_next_or_none(self):
        next_message = next(self._messages, None)
        return next_message

    def _has_seen_req_completed(self):
        return len(self._req_completed_messages) > 0

    def _has_seen_req_started(self):
        return self._req_started is not None

    def _process_message(self, message):
        if message.is_ehr_request_completed():
            self._req_completed_messages.append(message)
        elif (
            self._has_seen_req_completed()
            and self._is_acknowledging_any_request_completed_message(message)
        ):
            self._request_completed_ack_messages.append(message)
        elif self._has_seen_req_started() and message.is_acknowledgement_of(self._req_started):
            self._request_started_ack = message
        else:
            self._intermediate_messages.append(message)

    def _is_acknowledging_any_request_completed_message(self, message):
        return any(
            message.is_acknowledgement_of(req_completed_message)
            for req_completed_message in self._req_completed_messages
        )

    def _process_first_message(self):
        first_message = self._get_next_or_none()
        if first_message.is_ehr_request_started():
            self._req_started = first_message
        else:
            raise ConversationMissingStart()

    def parse(self):

        self._process_first_message()

        while (next_message := self._get_next_or_none()) is not None:
            self._process_message(next_message)

        return ParsedConversation(
            self._id,
            request_started=self._req_started,
            request_started_ack=self._request_started_ack,
            request_completed_messages=self._req_completed_messages,
            intermediate_messages=self._intermediate_messages,
            request_completed_ack_messages=self._request_completed_ack_messages,
        )


def filter_conversations_by_request_started_time(
    conversations: Iterable[ParsedConversation], time_range: DateTimeRange
) -> Iterator[ParsedConversation]:
    return (
        conversation
        for conversation in conversations
        if time_range.contains(conversation.request_started.time)
    )
