from typing import Iterable
from unittest.mock import Mock, call

from prmdata.domain.spine.message import Message
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string
from tests.builders.spine import build_message


def _spine_messages_as_dict(spine_messages: Iterable[Message]) -> Iterable[dict]:
    return [
        {
            "_time": message.time.isoformat(),
            "conversationID": message.conversation_id,
            "GUID": message.guid,
            "interactionID": message.interaction_id,
            "messageSender": message.from_party_asid,
            "messageRecipient": message.to_party_asid,
            "messageRef": message.message_ref,
            "fromSystem": message.from_system,
            "toSystem": message.to_system,
            "jdiEvent": "NONE" if message.error_code is None else str(message.error_code),
        }
        for message in spine_messages
    ]


def test_read_spine_messages():
    s3_manager = Mock()
    reporting_window = MonthlyReportingWindow.prior_to(a_datetime(year=2021, month=2))

    spine_message_one = build_message()
    spine_message_two = build_message()
    spine_message_three = build_message()
    spine_bucket = "test_spine_bucket"

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=spine_bucket,
        dashboard_data_bucket=a_string(),
    )

    s3_manager.read_gzip_csv.side_effect = [
        iter(_spine_messages_as_dict([spine_message_one, spine_message_two])),
        iter(_spine_messages_as_dict([spine_message_three])),
    ]

    expected_data = [spine_message_one, spine_message_two, spine_message_three]
    expected_path = f"s3://{spine_bucket}/v2/messages/2021/1/2021-1_spine_messages.csv.gz"
    expected_overflow_path = (
        f"s3://{spine_bucket}/v2/messages-overflow/2021/2/2021-2_spine_messages_overflow.csv.gz"
    )

    actual_data = list(metrics_io.read_spine_messages())

    assert actual_data == expected_data

    s3_manager.read_gzip_csv.assert_has_calls([call(expected_path), call(expected_overflow_path)])
