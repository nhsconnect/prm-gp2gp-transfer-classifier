from tests.builders.common import a_datetime
from tests.builders.spine import build_gp2gp_conversation, build_message
from prmdata.domain.spine.message import ERROR_SUPPRESSED


def test_returns_none_when_request_has_been_made():
    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=None,
        request_completed_messages=[],
        request_completed_ack_messages=[],
    )

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_none_when_request_has_been_acknowledged():
    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[],
        request_completed_ack_messages=[],
    )

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_none_when_ehr_has_been_returned():
    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[build_message()],
        request_completed_ack_messages=[],
    )

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_none_given_duplicate_and_pending():
    conversation = build_gp2gp_conversation(
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

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_when_conversation_has_concluded_successfully():

    effective_request_completed_time = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(time=effective_request_completed_time, guid="abc")
        ],
        request_completed_ack_messages=[build_message(message_ref="abc", error_code=None)],
    )

    expected = effective_request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_when_record_is_suppressed():
    effective_request_completed_time = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(time=effective_request_completed_time, guid="abc")
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc", error_code=ERROR_SUPPRESSED)
        ],
    )

    expected = effective_request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_when_conversation_concluded_with_failure():
    effective_request_completed_time = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(time=effective_request_completed_time, guid="abc")
        ],
        request_completed_ack_messages=[build_message(message_ref="abc", error_code=99)],
    )

    expected = effective_request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_duplicate_and_success():
    effective_request_completed_time = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="bcd"),
            build_message(time=effective_request_completed_time, guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc", error_code=None),
            build_message(message_ref="bcd", error_code=12),
        ],
    )

    expected = effective_request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_duplicate_and_failure():
    effective_request_completed_time = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="bcd"),
            build_message(time=effective_request_completed_time, guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="bcd", error_code=12),
            build_message(message_ref="abc", error_code=99),
        ],
    )

    expected = effective_request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_success_and_failure():
    effective_request_completed_time = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(
                guid="bcd",
                time=effective_request_completed_time,
            ),
            build_message(guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="bcd", error_code=None),
            build_message(message_ref="abc", error_code=99),
        ],
    )

    expected = effective_request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_failure_and_success():
    effective_request_completed_time = a_datetime()

    conversation = build_gp2gp_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="bcd", time=effective_request_completed_time),
            build_message(guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc", error_code=99),
            build_message(
                message_ref="bcd",
                error_code=None,
            ),
        ],
    )

    expected = effective_request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected
