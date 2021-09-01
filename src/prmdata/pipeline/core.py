from datetime import timedelta
from typing import Iterable, Iterator

from prmdata.domain.gp2gp.transfer import (
    Transfer,
)

from prmdata.domain.gp2gp.transfer_service import TransferService
from prmdata.domain.spine.message import Message
from prmdata.domain.spine.gp2gp_conversation import (
    ConversationMissingStart,
    filter_conversations_by_request_started_time,
    Gp2gpConversation,
)
from prmdata.domain.spine.conversation import group_into_conversations
from prmdata.utils.reporting_window import MonthlyReportingWindow


def _parse_conversations(conversations):
    for conversation in conversations:
        try:
            yield Gp2gpConversation(conversation.messages)
        except ConversationMissingStart:
            pass


def parse_transfers_from_messages(
    spine_messages: Iterable[Message],
    reporting_window: MonthlyReportingWindow,
    conversation_cutoff: timedelta,
) -> Iterator[Transfer]:
    conversations = group_into_conversations(spine_messages, conversation_cutoff)
    gp2gp_conversations = _parse_conversations(conversations)
    conversations_started_in_reporting_window = filter_conversations_by_request_started_time(
        gp2gp_conversations, reporting_window
    )

    transfer_service = TransferService()

    return transfer_service.convert_to_transfers(conversations_started_in_reporting_window)
