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
from prmdata.utils.reporting_window import MonthlyReportingWindow


def parse_transfers_from_messages(
    spine_messages: Iterable[Message],
    reporting_window: MonthlyReportingWindow,
    conversation_cutoff: timedelta,
) -> Iterator[Transfer]:
    transfer_service = TransferService(spine_messages, conversation_cutoff)

    conversations = transfer_service.group_into_conversations()
    gp2gp_conversations = transfer_service.parse_conversations_into_gp2gp_conversations(conversations)
    conversations_started_in_reporting_window = filter_conversations_by_request_started_time(
        gp2gp_conversations, reporting_window
    )

    return transfer_service.convert_to_transfers(conversations_started_in_reporting_window)
