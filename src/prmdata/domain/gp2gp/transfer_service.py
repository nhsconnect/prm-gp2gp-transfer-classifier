from collections import defaultdict
from datetime import timedelta
from logging import getLogger, Logger
from typing import List, Iterable, Iterator, Dict, Optional

from prmdata.domain.gp2gp.transfer import (
    Transfer,
    Practice,
)
from prmdata.domain.gp2gp.transfer_outcome import (
    TransferOutcome,
)
from prmdata.domain.spine.conversation import Conversation
from prmdata.domain.spine.gp2gp_conversation import (
    Gp2gpConversation,
    ConversationMissingStart,
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
        observability_probe: TransferObservabilityProbe,
    ):
        self._probe = observability_probe
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
        gp2gp_conversation_observability_probe = Gp2gpConversationObservabilityProbe(
            logger=module_logger
        )

        for conversation in conversations:
            try:
                yield Gp2gpConversation(
                    conversation.messages, gp2gp_conversation_observability_probe
                )
            except ConversationMissingStart:
                pass

    def convert_to_transfers(self, conversations: Iterator[Gp2gpConversation]):
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
        )


def _ignore_messages_sent_after(cutoff: timedelta, messages: List[Message]) -> List[Message]:
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


#
# # flake8: noqa: C901
# def _assign_transfer_outcome(
#     conversation: Gp2gpConversation, sla_duration: Optional[timedelta]
# ) -> TransferOutcome:
#     if conversation.is_integrated():
#         return _integrated_within_sla(sla_duration)
#     elif conversation.has_concluded_with_failure():
#         return _technical_failure(TransferFailureReason.FINAL_ERROR)
#     elif conversation.contains_copc_fragments():
#         return _copc_transfer_outcome(conversation)
#     elif conversation.contains_fatal_sender_error_code():
#         return _technical_failure(TransferFailureReason.FATAL_SENDER_ERROR)
#     elif conversation.is_missing_request_acknowledged():
#         return _technical_failure(TransferFailureReason.REQUEST_NOT_ACKNOWLEDGED)
#     elif conversation.is_missing_core_ehr():
#         return _technical_failure(TransferFailureReason.CORE_EHR_NOT_SENT)
#     elif conversation.contains_core_ehr_with_sender_error():
#         return _unclassified_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR)
#     else:
#         return _process_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED)
#
#
# def _copc_transfer_outcome(conversation: Gp2gpConversation) -> TransferOutcome:
#     if conversation.contains_unacknowledged_duplicate_ehr_and_copc_fragments():
#         return _unclassified_failure(TransferFailureReason.AMBIGUOUS_COPCS)
#     elif conversation.contains_copc_error() and not conversation.is_missing_copc_ack():
#         return _unclassified_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR)
#     elif conversation.is_missing_copc():
#         return _technical_failure(TransferFailureReason.COPC_NOT_SENT)
#     elif conversation.is_missing_copc_ack():
#         return _technical_failure(TransferFailureReason.COPC_NOT_ACKNOWLEDGED)
#     else:
#         return _process_failure(TransferFailureReason.TRANSFERRED_NOT_INTEGRATED)

#
# def _integrated_within_sla(sla_duration: Optional[timedelta]) -> TransferOutcome:
#     if sla_duration is not None and sla_duration <= timedelta(days=8):
#         return _integrated_on_time()
#     return _process_failure(TransferFailureReason.INTEGRATED_LATE)
#
#
# def _integrated_on_time() -> TransferOutcome:
#     return TransferOutcome(status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None)
#
#
# def _technical_failure(reason: TransferFailureReason) -> TransferOutcome:
#     return TransferOutcome(status=TransferStatus.TECHNICAL_FAILURE, failure_reason=reason)
#
#
# def _process_failure(reason: TransferFailureReason) -> TransferOutcome:
#     return TransferOutcome(status=TransferStatus.PROCESS_FAILURE, failure_reason=reason)
#
#
# def _unclassified_failure(reason: TransferFailureReason = None) -> TransferOutcome:
#     return TransferOutcome(
#         status=TransferStatus.UNCLASSIFIED_FAILURE,
#         failure_reason=reason,
#     )
