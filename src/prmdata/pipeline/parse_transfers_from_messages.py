from datetime import timedelta
from logging import getLogger
from typing import Iterable, Iterator

from prmdata.domain.datetime import MonthlyReportingWindow
from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.gp2gp.transfer_service import TransferObservabilityProbe, TransferService
from prmdata.domain.spine.gp2gp_conversation import filter_conversations_by_request_started_time
from prmdata.domain.spine.message import Message

module_logger = getLogger(__name__)


def parse_transfers_from_messages(
    spine_messages: Iterable[Message],
    reporting_window: MonthlyReportingWindow,
    conversation_cutoff: timedelta,
    observability_probe: TransferObservabilityProbe,
) -> Iterator[Transfer]:
    transfer_service = TransferService(
        message_stream=spine_messages,
        cutoff=conversation_cutoff,
        observability_probe=observability_probe,
    )

    conversations = transfer_service.group_into_conversations()
    gp2gp_conversations = transfer_service.parse_conversations_into_gp2gp_conversations(
        conversations
    )
    conversations_started_in_reporting_window = filter_conversations_by_request_started_time(
        gp2gp_conversations, reporting_window
    )

    return transfer_service.convert_to_transfers(conversations_started_in_reporting_window)
