from typing import Iterable, List, Iterator

from prmdata.domain.data_platform.national_metrics import (
    NationalMetricsPresentation,
    construct_national_metrics,
)
from prmdata.domain.data_platform.practice_metrics import (
    construct_practice_summaries,
    PracticeMetricsPresentation,
    construct_practice_metrics_presentation,
)
from prmdata.domain.gp2gp.practice_lookup import PracticeLookup
from prmdata.domain.ods_portal.models import PracticeDetails
from prmdata.domain.gp2gp.national_metrics import calculate_national_metrics
from prmdata.domain.gp2gp.transfer import (
    Transfer,
    derive_transfers,
    filter_for_successful_transfers,
)
from prmdata.domain.gp2gp.practice_metrics import calculate_sla_by_practice
from prmdata.domain.spine.message import Message
from prmdata.domain.spine.gp2gp_conversation import (
    parse_conversation,
    ConversationMissingStart,
    filter_conversations_by_request_started_time,
)
from prmdata.domain.spine.conversation import group_into_conversations
from prmdata.utils.reporting_window import MonthlyReportingWindow


def _parse_conversations(conversations):
    for conversation in conversations:
        try:
            yield parse_conversation(conversation)
        except ConversationMissingStart:
            pass


def parse_transfers_from_messages(
    spine_messages: Iterable[Message], reporting_window: MonthlyReportingWindow
) -> Iterator[Transfer]:
    conversations = group_into_conversations(spine_messages)
    gp2gp_conversations = _parse_conversations(conversations)
    conversations_started_in_range = filter_conversations_by_request_started_time(
        gp2gp_conversations, reporting_window
    )
    transfers = derive_transfers(conversations_started_in_range)
    return transfers


def calculate_practice_metrics_data(
    transfers: List[Transfer],
    practice_list: List[PracticeDetails],
    reporting_window: MonthlyReportingWindow,
) -> PracticeMetricsPresentation:
    completed_transfers = filter_for_successful_transfers(transfers)
    practice_lookup = PracticeLookup(practice_list)
    sla_metrics = calculate_sla_by_practice(practice_lookup, completed_transfers)
    practice_summaries = construct_practice_summaries(
        sla_metrics, year=reporting_window.metric_year, month=reporting_window.metric_month
    )
    practice_metrics_presentation = construct_practice_metrics_presentation(practice_summaries)
    return practice_metrics_presentation


def calculate_national_metrics_data(
    transfers: List[Transfer], reporting_window: MonthlyReportingWindow
) -> NationalMetricsPresentation:
    national_metrics = calculate_national_metrics(transfers=transfers)
    return construct_national_metrics(
        national_metrics=national_metrics,
        year=reporting_window.metric_year,
        month=reporting_window.metric_month,
    )
