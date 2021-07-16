from typing import NamedTuple, List, Optional, Iterable, Iterator
from datetime import datetime
from warnings import warn

from prmdata.domain.spine.message import (
    Message,
    DUPLICATE_ERROR,
    ERROR_SUPPRESSED,
    FATAL_SENDER_ERROR_CODES,
)
from prmdata.utils.reporting_window import MonthlyReportingWindow


class Gp2gpConversation(NamedTuple):
    id: str
    request_started: Message
    request_started_ack: Optional[Message]
    request_completed_messages: List[Message]
    copc_continue: Optional[Message]
    copc_messages: List[Message]
    copc_ack_messages: List[Message]
    request_completed_ack_messages: List[Message]

    def conversation_id(self) -> str:
        return self.request_started.conversation_id

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
            for message in self.copc_ack_messages
            if message.error_code is not None
        ]

    def date_requested(self) -> datetime:
        return self.request_started.time

    def is_integrated(self) -> bool:
        final_ack = self._find_effective_request_completed_ack_message()
        has_final_ack = final_ack is not None
        return has_final_ack and _integrated_or_suppressed(final_ack)

    def has_concluded_with_failure(self) -> bool:
        final_ack = self._find_effective_request_completed_ack_message()
        has_final_ack = final_ack is not None
        return has_final_ack and not _integrated_or_suppressed(final_ack)

    def is_missing_final_ack(self) -> bool:
        final_ack = self._find_effective_request_completed_ack_message()
        return final_ack is None

    def is_missing_request_acknowledged(self) -> bool:
        missing_request_acknowledged = (
            self.request_started is not None and self.request_started_ack is None
        )
        return missing_request_acknowledged

    def is_missing_core_ehr(self) -> bool:
        is_missing_request_acknowledged = self.is_missing_request_acknowledged()
        return (
            is_missing_request_acknowledged is False and len(self.request_completed_messages) == 0
        )

    def is_missing_copc(self) -> bool:
        return self.copc_continue is not None and len(self.copc_messages) == 0

    def is_missing_copc_ack(self) -> bool:
        copc_ids = [copc.guid for copc in self.copc_messages]
        copc_ack_ids = [copc_ack.message_ref for copc_ack in self.copc_ack_messages]
        missing_copc_ack_ids = set(copc_ids) - set(copc_ack_ids)
        return len(missing_copc_ack_ids) > 0

    def contains_copc_error(self) -> bool:
        final_ack = self._find_effective_request_completed_ack_message()
        is_missing_copc_ack = self.is_missing_copc_ack()
        missing_final_ack = final_ack is None
        intermediate_error_codes = self.intermediate_error_codes()
        return missing_final_ack and len(intermediate_error_codes) > 0 and not is_missing_copc_ack

    def contains_fatal_sender_error_code(self) -> bool:
        final_ack = self._find_effective_request_completed_ack_message()
        missing_final_ack = final_ack is None
        has_fatal_sender_error = self.sender_error() in FATAL_SENDER_ERROR_CODES
        return missing_final_ack and has_fatal_sender_error

    def contains_core_ehr_with_sender_error(self) -> bool:
        final_ack = self._find_effective_request_completed_ack_message()
        missing_final_ack = final_ack is None
        is_missing_core_ehr = self.is_missing_core_ehr()
        has_sender_error = self.sender_error() is not None
        return missing_final_ack and has_sender_error and not is_missing_core_ehr

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

    def effective_final_acknowledgement_time(self) -> Optional[datetime]:
        final_ack = self._find_effective_request_completed_ack_message()
        if final_ack is None:
            return None
        return final_ack.time

    def _find_successful_request_completed_ack_message(self) -> Optional[Message]:
        return next(
            (
                message
                for message in self.request_completed_ack_messages
                if _integrated_or_suppressed(message)
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

    @classmethod
    def from_messages(cls, messages):
        parser = SpineConversationParser(messages)
        return parser.parse()

    def contains_copc_messages(self) -> bool:
        contains_copcs_messages = len(self.copc_messages) > 0
        return contains_copcs_messages or self.copc_continue is not None


def _integrated_or_suppressed(request_completed_ack) -> bool:
    return (
        request_completed_ack.error_code is None
        or request_completed_ack.error_code == ERROR_SUPPRESSED
    )


class ConversationMissingStart(Exception):
    pass


class SpineConversationParser:
    def __init__(self, messages: Iterable[Message]):
        self._messages = iter(messages)
        self._req_started: Optional[Message] = None
        self._req_completed_messages: List[Message] = []
        self._request_started_ack: Optional[Message] = None
        self._copc_continue: Optional[Message] = None
        self._copc_messages: List[Message] = []
        self._copc_ack_messages: List[Message] = []
        self._request_completed_ack_messages: List[Message] = []

    def _get_next_or_none(self) -> Message:
        next_message = next(self._messages, None)
        return next_message

    def _has_seen_req_completed(self) -> bool:
        return len(self._req_completed_messages) > 0

    def _has_seen_req_started(self) -> bool:
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
        elif message.is_copc():
            self._process_copc(message)
        else:
            self._copc_ack_messages.append(message)

    def _process_copc(self, message: Message):
        if self._is_copc_continue(message):
            if self._copc_continue is not None:
                warn(
                    f"Duplicate COPC Continue found in conversation: {message.conversation_id}",
                    RuntimeWarning,
                )
            else:
                self._copc_continue = message
        else:
            self._copc_messages.append(message)

    def _is_acknowledging_any_request_completed_message(self, message: Message) -> bool:
        return any(
            message.is_acknowledgement_of(req_completed_message)
            for req_completed_message in self._req_completed_messages
        )

    def _is_copc_continue(self, message: Message) -> bool:
        if self._req_started:
            requesting_practice_asid = self._req_started.from_party_asid
            return message.from_party_asid == requesting_practice_asid
        else:
            return False

    def _process_first_message(self) -> str:
        first_message = self._get_next_or_none()
        if first_message.is_ehr_request_started():
            self._req_started = first_message
            return first_message.conversation_id
        else:
            raise ConversationMissingStart()

    def parse(self) -> Gp2gpConversation:

        conversation_id = self._process_first_message()

        while (next_message := self._get_next_or_none()) is not None:
            self._process_message(next_message)

        return Gp2gpConversation(
            id=conversation_id,
            request_started=self._req_started,
            request_started_ack=self._request_started_ack,
            request_completed_messages=self._req_completed_messages,
            copc_continue=self._copc_continue,
            copc_messages=self._copc_messages,
            copc_ack_messages=self._copc_ack_messages,
            request_completed_ack_messages=self._request_completed_ack_messages,
        )


def filter_conversations_by_request_started_time(
    conversations: Iterable[Gp2gpConversation], reporting_window: MonthlyReportingWindow
) -> Iterator[Gp2gpConversation]:
    return (
        conversation
        for conversation in conversations
        if reporting_window.contains(conversation.request_started.time)
    )
