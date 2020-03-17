from typing import Iterable, List

from gp2gp.dashboard.models import ServiceDashboardData
from gp2gp.dashboard.transformers import construct_service_dashboard_data
from gp2gp.date.range import DateTimeRange
from gp2gp.odsportal.models import PracticeDetails
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
)


def _parse_conversations(conversations):
    for conversation in conversations:
        gp2gp_conversation = parse_conversation(conversation)
        if gp2gp_conversation is not None:
            yield gp2gp_conversation


def calculate_dashboard_data(
    spine_messages: Iterable[Message],
    practice_list: List[PracticeDetails],
    time_range: DateTimeRange,
) -> ServiceDashboardData:
    conversations = group_into_conversations(spine_messages)
    parsed_conversations = _parse_conversations(conversations)
    conversations_started_in_range = filter_conversations_by_request_started_time(
        parsed_conversations, time_range
    )
    transfers = derive_transfers(conversations_started_in_range)
    completed_transfers = filter_for_successful_transfers(transfers)
    sla_metrics = calculate_sla_by_practice(practice_list, completed_transfers)
    dashboard_data = construct_service_dashboard_data(
        sla_metrics, year=time_range.start.year, month=time_range.start.month
    )
    return dashboard_data
