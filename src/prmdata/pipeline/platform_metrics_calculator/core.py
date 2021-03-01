from typing import Iterable, List, Iterator

from prmdata.domain.data_platform.national_metrics import (
    NationalMetricsPresentation,
    construct_national_metrics,
)
from prmdata.domain.data_platform.practice_metrics import (
    construct_practice_metrics,
    PracticeMetricsPresentation,
)
from prmdata.utils.date.range import DateTimeRange
from prmdata.domain.ods_portal.models import PracticeDetails
from prmdata.domain.gp2gp.national_metrics import calculate_national_metrics
from prmdata.domain.gp2gp.transfer import (
    Transfer,
    derive_transfers,
    filter_for_successful_transfers,
)
from prmdata.domain.gp2gp.practice_metrics import calculate_sla_by_practice
from prmdata.domain.spine.models import Message
from prmdata.domain.spine.transformers import (
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
    practice_metrics = construct_practice_metrics(
        sla_metrics, year=time_range.start.year, month=time_range.start.month
    )
    return practice_metrics


def calculate_national_metrics_data(
    transfers: Iterable[Transfer], time_range: DateTimeRange
) -> NationalMetricsPresentation:
    national_metrics_by_month = calculate_national_metrics(transfers=transfers)
    return construct_national_metrics(
        national_metrics_by_month=national_metrics_by_month,
        year=time_range.start.year,
        month=time_range.start.month,
    )
