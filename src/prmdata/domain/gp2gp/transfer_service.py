from collections import defaultdict
from datetime import timedelta
from logging import Logger, getLogger
from typing import Dict, Iterable, Iterator, List, Optional

from prmdata.domain.gp2gp.transfer import Practice, Transfer
from prmdata.domain.gp2gp.transfer_outcome import TransferOutcome
from prmdata.domain.spine.conversation import Conversation
from prmdata.domain.spine.gp2gp_conversation import (
    ConversationMissingStart,
    Gp2gpConversation,
    Gp2gpConversationObservabilityProbe,
)
from prmdata.domain.spine.message import Message

module_logger = getLogger(__name__)


class TransferObservabilityProbe:
    def __init__(self, logger: Logger = module_logger):
        self._logger = logger

    def record_negative_sla(self, conversation: Gp2gpConversation):
        self._logger.warning(
            f":Negative SLA duration for conversation: {conversation.conversation_id()}",
            extra={
                "event": "NEGATIVE_SLA_DETECTED",
                "conversation_id": conversation.conversation_id(),
                "final_acknowledgement_time": conversation.effective_final_acknowledgement_time(),
                "request_completed_time": conversation.effective_request_completed_time(),
            },
        )


class TransferService:
    def __init__(
        self,
        message_stream: Iterable[Message],
        cutoff: timedelta,
    ):
        self._probe = TransferObservabilityProbe(logger=module_logger)
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
    def parse_conversations_into_gp2gp_conversations(
        conversations: Iterator[Conversation],
    ) -> List[Gp2gpConversation]:
        gp2gp_conversation_observability_probe = Gp2gpConversationObservabilityProbe(
            logger=module_logger
        )
        gp2gp_conversations = []

        for conversation in conversations:
            try:
                gp2gp_conversation = Gp2gpConversation(
                    conversation.messages, gp2gp_conversation_observability_probe
                )
                gp2gp_conversations.append(gp2gp_conversation)
            except ConversationMissingStart:
                pass

        return gp2gp_conversations

    def convert_to_transfers(
        self, conversations: Iterator[Gp2gpConversation]
    ) -> Iterator[Transfer]:
        return (self.derive_transfer(conversation) for conversation in conversations)

    def derive_transfer(
        self,
        conversation: Gp2gpConversation,
    ) -> Transfer:
        sla_duration = _calculate_sla(conversation, self._probe)
        return Transfer(
            conversation_id=conversation.conversation_id(),
            sla_duration=sla_duration,
            requesting_practice=Practice(
                asid=conversation.requesting_practice_asid(),
                supplier=conversation.requesting_supplier(),
            ),
            sending_practice=Practice(
                asid=conversation.sending_practice_asid(), supplier=conversation.sending_supplier()
            ),
            sender_error_codes=conversation.sender_error_codes(),
            final_error_codes=conversation.final_error_codes(),
            intermediate_error_codes=conversation.intermediate_error_codes(),
            outcome=TransferOutcome.from_gp2gp_conversation(conversation, sla_duration),
            date_requested=conversation.date_requested(),
            date_completed=conversation.effective_final_acknowledgement_time(),
            last_sender_message_timestamp=conversation.last_sender_message_timestamp(),
        )


def _ignore_messages_sent_after(cutoff: timedelta, messages: List[Message]) -> List[Message]:
    if cutoff.total_seconds() == 0:
        return messages

    first_message_in_conversation = messages[0]
    start_of_conversation = first_message_in_conversation.time
    return [message for message in messages if message.time - start_of_conversation <= cutoff]


def _calculate_sla(
    conversation: Gp2gpConversation, probe: TransferObservabilityProbe
) -> Optional[timedelta]:
    final_acknowledgement_time = conversation.effective_final_acknowledgement_time()
    request_completed_time = conversation.effective_request_completed_time()

    if final_acknowledgement_time is None:
        return None

    sla_duration = final_acknowledgement_time - request_completed_time

    if sla_duration.total_seconds() < 0:
        probe.record_negative_sla(conversation)

    return max(timedelta(0), sla_duration)
