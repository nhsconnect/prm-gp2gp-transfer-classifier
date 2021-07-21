from typing import NamedTuple, List, Optional, Iterable, Iterator, Tuple
from datetime import datetime
from warnings import warn

from prmdata.domain.spine.message import (
    Message,
    DUPLICATE_ERROR,
    ERROR_SUPPRESSED,
    FATAL_SENDER_ERROR_CODES,
)
from prmdata.utils.reporting_window import MonthlyReportingWindow


class AcknowledgedMessage(NamedTuple):
    message: Message
    acknowledgements: List[Message]

    def has_acknowledgements(self) -> bool:
        return len(self.acknowledgements) > 0

    def from_asid(self) -> str:
        return self.message.from_party_asid

    def to_asid(self) -> str:
        return self.message.to_party_asid

    def is_sent_by(self, asid):
        return self.message.from_party_asid == asid

    def is_ehr_request_completed(self):
        return self.message.is_ehr_request_completed()

    def is_copc(self):
        return self.message.is_copc()


class Gp2gpConversation:
    def __init__(self, messages: List[Message]):

        first_message = messages[0]
        if not first_message.is_ehr_request_started():
            raise ConversationMissingStart()

        acked_messages = _pair_messages_with_acks(messages)
        message_bundle = _group_message_by_type(acked_messages)

        self._request_started = message_bundle.request_started
        self._request_completed = message_bundle.request_completed
        self._large_message_continue = message_bundle.large_messaging_continue
        self._large_fragments = message_bundle.large_fragments

        self._effective_ehr = None
        self._effective_ehr_ack = None

        effective_request_completed = _find_effective_request_completed(self._request_completed)
        if effective_request_completed is not None:
            (self._effective_ehr, self._effective_ehr_ack) = effective_request_completed

    def conversation_id(self) -> str:
        return self._request_started.message.conversation_id

    def sending_practice_asid(self) -> str:
        return self._request_started.to_asid()

    def requesting_practice_asid(self) -> str:
        return self._request_started.from_asid()

    def requesting_supplier(self) -> str:
        return self._request_started.message.from_system

    def sending_supplier(self) -> str:
        return self._request_started.message.to_system

    def final_error_codes(self) -> List[Optional[int]]:
        return [
            ack.error_code
            for message in self._request_completed
            for ack in message.acknowledgements
        ]

    def sender_error(self) -> Optional[int]:
        return next((ack.error_code for ack in self._request_started.acknowledgements), None)

    def intermediate_error_codes(self) -> List[int]:
        return [
            ack.error_code
            for message in self._large_fragments
            for ack in message.acknowledgements
            if ack.error_code is not None
        ]

    def date_requested(self) -> datetime:
        return self._request_started.message.time

    def is_integrated(self) -> bool:
        return self._effective_ehr_ack is not None and _integrated_or_suppressed(
            self._effective_ehr_ack
        )

    def has_concluded_with_failure(self) -> bool:
        return self._effective_ehr_ack is not None and not _integrated_or_suppressed(
            self._effective_ehr_ack
        )

    def is_missing_final_ack(self) -> bool:
        return self._effective_ehr_ack is None

    def is_missing_request_acknowledged(self) -> bool:
        return not self._request_started.has_acknowledgements()

    def is_missing_core_ehr(self) -> bool:
        return len(self._request_completed) == 0

    def is_missing_copc(self) -> bool:
        return len(self._large_message_continue) > 0 and len(self._large_fragments) == 0

    def is_missing_copc_ack(self) -> bool:
        return any(not message.has_acknowledgements() for message in self._large_fragments)

    def contains_copc_error(self) -> bool:
        return any(
            ack.error_code is not None
            for message in self._large_fragments
            for ack in message.acknowledgements
        )

    def contains_fatal_sender_error_code(self) -> bool:
        return any(
            ack.error_code in FATAL_SENDER_ERROR_CODES
            for ack in self._request_started.acknowledgements
        )

    def contains_core_ehr_with_sender_error(self) -> bool:
        contains_any_sender_error = any(
            ack.error_code is not None for ack in self._request_started.acknowledgements
        )
        return not self.is_missing_core_ehr() and contains_any_sender_error

    def effective_request_completed_time(self) -> Optional[datetime]:
        return self._effective_ehr.time if self._effective_ehr else None

    def effective_final_acknowledgement_time(self) -> Optional[datetime]:
        return self._effective_ehr_ack.time if self._effective_ehr_ack else None

    def contains_copc_messages(self) -> bool:
        return len(self._large_message_continue) > 0 or len(self._large_fragments) > 0

    def contains_unacknowledged_duplicate_ehr_and_copcs(self) -> bool:
        has_duplicates = self._count_duplicate_errors() > 0
        contains_copcs = self.contains_copc_messages()

        return has_duplicates and contains_copcs and not self._all_ehr_acknowledged()

    def contains_only_duplicate_ehr(self) -> bool:
        return self._all_ehr_acknowledged() and self._all_ehr_acks_are_duplicates()

    def _all_ehr_acknowledged(self):
        return all(message.has_acknowledgements() for message in self._request_completed)

    def _all_ehr_acks_are_duplicates(self):
        return all(
            ack.error_code == DUPLICATE_ERROR
            for message in self._request_completed
            for ack in message.acknowledgements
        )

    def _count_duplicate_errors(self):
        return self.final_error_codes().count(DUPLICATE_ERROR)


def _integrated_or_suppressed(request_completed_ack) -> bool:
    return (
        request_completed_ack.error_code is None
        or request_completed_ack.error_code == ERROR_SUPPRESSED
    )


class ConversationMissingStart(Exception):
    pass


class Gp2gpMessagesByType(NamedTuple):
    request_started: AcknowledgedMessage
    large_messaging_continue: List[Message]
    large_fragments: List[AcknowledgedMessage]
    request_completed: List[AcknowledgedMessage]


def _find_acked_ehr_completed_where_ack(request_completed_messages, predicate):
    return next(
        (
            (request_completed.message, ack)
            for request_completed in request_completed_messages
            for ack in request_completed.acknowledgements
            if predicate(ack)
        ),
        None,
    )


def _find_effective_request_completed(
    request_completed_messages,
) -> Optional[Tuple[Message, Message]]:
    successfully_acked_ehr = _find_acked_ehr_completed_where_ack(
        request_completed_messages, _integrated_or_suppressed
    )

    if successfully_acked_ehr is not None:
        return successfully_acked_ehr

    unsuccessfully_acked_ehr = _find_acked_ehr_completed_where_ack(
        request_completed_messages, lambda ack: ack.error_code != DUPLICATE_ERROR
    )

    if unsuccessfully_acked_ehr is not None:
        return unsuccessfully_acked_ehr

    return None


def _group_message_by_type(messages: Iterable[AcknowledgedMessage]) -> Gp2gpMessagesByType:
    request_completed_messages = []
    large_messaging_continue_messages = []
    large_fragment_messages = []

    request_started, *remaining_messages = messages

    requesting_asid = request_started.from_asid()
    sending_asid = request_started.to_asid()

    for acked_message in remaining_messages:
        if acked_message.is_ehr_request_completed():
            request_completed_messages.append(acked_message)
        elif acked_message.is_copc() and acked_message.is_sent_by(requesting_asid):
            large_messaging_continue_messages.append(acked_message.message)
        elif acked_message.message.is_copc() and acked_message.is_sent_by(sending_asid):
            large_fragment_messages.append(acked_message)
        else:
            warn(f"Couldn't determine purpose of message: {acked_message.message.guid}")

    return Gp2gpMessagesByType(
        request_started=request_started,
        request_completed=request_completed_messages,
        large_messaging_continue=large_messaging_continue_messages,
        large_fragments=large_fragment_messages,
    )


def _pair_messages_with_acks(messages: Iterable[Message]) -> List[AcknowledgedMessage]:
    acked_messages: dict[str, AcknowledgedMessage] = {}

    for message in messages:
        if message.is_acknowledgement():
            try:
                acked_messages[message.message_ref].acknowledgements.append(message)
            except KeyError:
                warn(f"Couldn't pair acknowledgement with message for ref: {message.message_ref}")
        else:
            acked_messages[message.guid] = AcknowledgedMessage(message=message, acknowledgements=[])

    return list(acked_messages.values())


def filter_conversations_by_request_started_time(
    conversations: Iterable[Gp2gpConversation], reporting_window: MonthlyReportingWindow
) -> Iterator[Gp2gpConversation]:
    return (
        conversation
        for conversation in conversations
        if reporting_window.contains(conversation.date_requested())
    )
