from tests.builders.common import a_datetime
from tests.builders.spine import build_parsed_conversation, build_message
from prmdata.domain.spine.message import ERROR_SUPPRESSED


def test_effective_request_completed_time_returns_none_when_request_has_been_made():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=None,
        request_completed_messages=[],
        request_completed_ack_messages=[],
    )

    actual = conversation.effective_request_completed_time()
    expected_effective_request_completed_time = None

    assert actual == expected_effective_request_completed_time


def test_effective_request_completed_time_returns_none_when_request_has_been_acknowledged():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[],
        request_completed_ack_messages=[],
    )

    actual = conversation.effective_request_completed_time()
    expected_effective_request_completed_time = None

    assert actual == expected_effective_request_completed_time


def test_effective_request_completed_time_returns_none_when_ehr_has_been_returned():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[build_message()],
        request_completed_ack_messages=[],
    )

    actual = conversation.effective_request_completed_time()
    expected_effective_request_completed_time = None

    assert actual == expected_effective_request_completed_time


def test_effective_request_completed_time_returns_time_when_conversation_has_concluded():

    effective_request_completed_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(time=effective_request_completed_time, guid="abc")
        ],
        request_completed_ack_messages=[build_message(message_ref="abc")],
    )

    actual = conversation.effective_request_completed_time()

    assert actual == effective_request_completed_time


def test_effective_request_completed_time_returns_time_when_record_is_suppressed():
    effective_request_completed_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(time=effective_request_completed_time, guid="abc")
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc", error_code=ERROR_SUPPRESSED)
        ],
    )

    actual = conversation.effective_request_completed_time()

    assert actual == effective_request_completed_time


def test_effective_request_completed_time_returns_time_when_conversation_concluded_with_failure():
    effective_request_completed_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(time=effective_request_completed_time, guid="abc")
        ],
        request_completed_ack_messages=[build_message(message_ref="abc", error_code=99)],
    )

    actual = conversation.effective_request_completed_time()

    assert actual == effective_request_completed_time


def test_effective_request_completed_time_returns_none_given_duplicate_and_pending():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="abc"),
            build_message(guid="bcd"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc", error_code=12),
        ],
    )

    actual = conversation.effective_request_completed_time()

    effective_request_completed_time = None

    assert actual == effective_request_completed_time


def test_effective_request_completed_time_returns_time_given_duplicate_and_success():
    effective_request_completed_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="bcd"),
            build_message(time=effective_request_completed_time, guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc"),
            build_message(message_ref="bcd", error_code=12),
        ],
    )

    actual = conversation.effective_request_completed_time()

    assert actual == effective_request_completed_time


def test_effective_request_completed_time_returns_time_given_success_and_fail():
    effective_request_completed_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="bcd"),
            build_message(time=effective_request_completed_time, guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc"),
            build_message(message_ref="bcd", error_code=12),
        ],
    )

    actual = conversation.effective_request_completed_time()

    assert actual == effective_request_completed_time
