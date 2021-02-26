from typing import Iterable, List, Iterator

from gp2gp.domain.dashboard.national_metrics import (
    NationalMetricsPresentation,
    construct_national_metrics,
)
from gp2gp.domain.dashboard.practice_metrics import (
    construct_practice_metrics,
    PracticeMetricsPresentation,
)
from gp2gp.utils.date.range import DateTimeRange
from gp2gp.domain.ods_portal.models import PracticeDetails
from gp2gp.domain.service.national_metrics import calculate_national_metrics
from gp2gp.domain.service.transfer import (
    Transfer,
    derive_transfers,
    filter_for_successful_transfers,
)
from gp2gp.domain.service.practice_metrics import calculate_sla_by_practice
from gp2gp.domain.spine.models import Message
from gp2gp.domain.spine.transformers import (
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


def calculate_practice_metrics_data(
    transfers: Iterable[Transfer],
    practice_list: List[PracticeDetails],
    time_range: DateTimeRange,
) -> PracticeMetricsPresentation:
    completed_transfers = filter_for_successful_transfers(transfers)
    sla_metrics = calculate_sla_by_practice(practice_list, completed_transfers)
    dashboard_data = construct_practice_metrics(
        sla_metrics, year=time_range.start.year, month=time_range.start.month
    )
    return dashboard_data


def calculate_national_metrics_data(
    transfers: Iterable[Transfer], time_range: DateTimeRange
) -> NationalMetricsPresentation:
    national_metrics_by_month = calculate_national_metrics(
        transfers=transfers, year=time_range.start.year, month=time_range.start.month
    )
    return construct_national_metrics(national_metrics_by_month=national_metrics_by_month)
