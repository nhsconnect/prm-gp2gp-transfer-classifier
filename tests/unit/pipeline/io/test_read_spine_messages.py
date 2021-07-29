from datetime import datetime
from unittest.mock import Mock, call

from dateutil.tz import tzutc

from prmdata.domain.spine.message import Message
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string

_MESSAGE_TIME = datetime(2019, 12, 1, 8, 41, 48, 337000, tzinfo=tzutc())
_MESSAGE_TIME_STR = "2019-12-01T08:41:48.337+0000"


def _spine_message_obj_number(number):
    return Message(
        time=_MESSAGE_TIME,
        guid=f"guid{number}",
        conversation_id=f"conversation{number}",
        interaction_id=f"interaction{number}",
        from_party_asid=f"fromasid{number}",
        to_party_asid=f"toasid{number}",
        message_ref=f"ref{number}",
        error_code=None,
        from_system=f"fromsys{number}",
        to_system=f"tosys{number}",
    )


def _spine_message_dict_number(number):
    return {
        "_time": _MESSAGE_TIME_STR,
        "conversationID": f"conversation{number}",
        "GUID": f"guid{number}",
        "interactionID": f"interaction{number}",
        "messageSender": f"fromasid{number}",
        "messageRecipient": f"toasid{number}",
        "messageRef": f"ref{number}",
        "fromSystem": f"fromsys{number}",
        "toSystem": f"tosys{number}",
        "jdiEvent": "NONE",
    }


def test_read_spine_messages():
    s3_manager = Mock()
    reporting_window = MonthlyReportingWindow.prior_to(a_datetime(year=2021, month=2))

    spine_message_one = _spine_message_obj_number(1)
    spine_message_two = _spine_message_obj_number(2)
    spine_message_three = _spine_message_obj_number(3)
    spine_bucket = "test_spine_bucket"

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        gp2gp_spine_bucket=spine_bucket,
        dashboard_data_bucket=a_string(),
    )

    s3_manager.read_gzip_csv.side_effect = [
        iter([_spine_message_dict_number(1), _spine_message_dict_number(2)]),
        iter([_spine_message_dict_number(3)]),
    ]

    expected_data = [spine_message_one, spine_message_two, spine_message_three]
    expected_path = f"s3://{spine_bucket}/v2/messages/2021/1/2021-1_spine_messages.csv.gz"
    expected_overflow_path = (
        f"s3://{spine_bucket}/v2/messages-overflow/2021/2/2021-2_spine_messages_overflow.csv.gz"
    )

    actual_data = list(metrics_io.read_spine_messages())

    assert actual_data == expected_data

    s3_manager.read_gzip_csv.assert_has_calls([call(expected_path), call(expected_overflow_path)])
