from datetime import timedelta
from typing import Iterable, List, Iterator, Optional

from prmdata.domain.data_platform.national_metrics import (
    NationalMetricsPresentation,
    construct_national_metrics,
)
from prmdata.domain.gp2gp.national_metrics import calculate_national_metrics
from prmdata.domain.gp2gp.transfer import (
    Transfer,
    derive_transfer,
)
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
    conversation_cutoff: Optional[timedelta] = None,
) -> Iterator[Transfer]:
    conversations = group_into_conversations(spine_messages, conversation_cutoff)
    gp2gp_conversations = _parse_conversations(conversations)
    conversations_started_in_reporting_window = filter_conversations_by_request_started_time(
        gp2gp_conversations, reporting_window
    )
    transfers = (
        derive_transfer(conversation)
        for conversation in (conversations_started_in_reporting_window)
    )
    return transfers


def calculate_national_metrics_data(
    transfers: List[Transfer], reporting_window: MonthlyReportingWindow
) -> NationalMetricsPresentation:
    national_metrics = calculate_national_metrics(transfers=transfers)
    return construct_national_metrics(
        national_metrics=national_metrics,
        year=reporting_window.metric_year,
        month=reporting_window.metric_month,
    )
