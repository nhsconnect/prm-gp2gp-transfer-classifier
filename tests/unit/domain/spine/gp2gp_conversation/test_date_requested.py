from tests.builders.common import a_datetime
from tests.builders.spine import build_gp2gp_conversation, build_message


def test_extracts_date_requested_from_request_started_message():
    date_requested = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(time=date_requested),
        request_completed_messages=[build_message()],
        request_completed_ack_messages=[build_message()],
    )

    actual = conversation.date_requested()

    assert actual == date_requested
