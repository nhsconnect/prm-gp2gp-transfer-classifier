from typing import Iterable, List, Iterator

from gp2gp.dashboard.models import ServiceDashboardData
from gp2gp.dashboard.transformers import construct_service_dashboard_data
from gp2gp.date.range import DateTimeRange
from gp2gp.odsportal.models import PracticeDetails
from gp2gp.service.transfer import Transfer
from gp2gp.service.transformers import (
    derive_transfers,
    filter_for_successful_transfers,
    calculate_sla_by_practice,
)
from gp2gp.spine.models import Message
from gp2gp.spine.transformers import (
    parse_conversation,
    group_into_conversations,
    filter_conversations_by_request_started_time,
    ConversationMissingStart,
)


def _parse_conversations(conversations):
    for conversation in conversations:
        try:
            yield parse_conversation(conversation)
        except ConversationMissingStart:
            pass


def parse_transfers_from_messages(
    spine_messages: Iterable[Message], time_range: DateTimeRange
) -> Iterator[Transfer]:
    conversations = group_into_conversations(spine_messages)
    parsed_conversations = _parse_conversations(conversations)
    conversations_started_in_range = filter_conversations_by_request_started_time(
        parsed_conversations, time_range
    )
    transfers = derive_transfers(conversations_started_in_range)
    return transfers


def calculate_dashboard_data(
    transfers: Iterable[Transfer],
    practice_list: List[PracticeDetails],
    time_range: DateTimeRange,
) -> ServiceDashboardData:
    completed_transfers = filter_for_successful_transfers(transfers)
    sla_metrics = calculate_sla_by_practice(practice_list, completed_transfers)
    dashboard_data = construct_service_dashboard_data(
        sla_metrics, year=time_range.start.year, month=time_range.start.month
    )
    return dashboard_data
