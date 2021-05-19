from prmdata.domain.spine.message import ERROR_SUPPRESSED
from tests.builders.common import a_datetime
from tests.builders.spine import build_parsed_conversation, build_message


def test_returns_none_when_request_has_been_made():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=None,
        request_completed_messages=[],
        request_completed_ack_messages=[],
    )

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_none_when_request_has_been_acknowledged():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[],
        request_completed_ack_messages=[],
    )

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_none_when_ehr_has_been_returned():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[build_message()],
        request_completed_ack_messages=[],
    )

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_none_given_duplicate_and_pending():
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

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_time_when_conversation_has_concluded_successfully():
    effective_final_acknowledgement_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[build_message(guid="abc")],
        request_completed_ack_messages=[
            build_message(
                message_ref="abc", error_code=None, time=effective_final_acknowledgement_time
            )
        ],
    )

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_time_when_record_is_suppressed():
    effective_final_acknowledgement_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[build_message(guid="abc")],
        request_completed_ack_messages=[
            build_message(
                message_ref="abc",
                error_code=ERROR_SUPPRESSED,
                time=effective_final_acknowledgement_time,
            )
        ],
    )

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_time_when_conversation_concluded_with_failure():
    effective_final_acknowledgement_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[build_message(guid="abc")],
        request_completed_ack_messages=[
            build_message(
                message_ref="abc", error_code=99, time=effective_final_acknowledgement_time
            )
        ],
    )

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_time_given_duplicate_and_success():
    effective_final_acknowledgement_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="bcd"),
            build_message(guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(
                message_ref="abc", error_code=None, time=effective_final_acknowledgement_time
            ),
            build_message(message_ref="bcd", error_code=12),
        ],
    )

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_time_given_duplicate_and_failure():
    effective_final_acknowledgement_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(guid="bcd"),
            build_message(guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="bcd", error_code=12),
            build_message(
                message_ref="abc", error_code=99, time=effective_final_acknowledgement_time
            ),
        ],
    )

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_time_given_success_and_failure():
    effective_final_acknowledgement_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(
                guid="bcd",
            ),
            build_message(guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(
                message_ref="bcd", error_code=None, time=effective_final_acknowledgement_time
            ),
            build_message(message_ref="abc", error_code=99),
        ],
    )

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_time_given_failure_and_success():
    effective_final_acknowledgement_time = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_started_ack=build_message(),
        request_completed_messages=[
            build_message(
                guid="bcd",
            ),
            build_message(guid="abc"),
        ],
        request_completed_ack_messages=[
            build_message(message_ref="abc", error_code=99),
            build_message(
                message_ref="bcd", error_code=None, time=effective_final_acknowledgement_time
            ),
        ],
    )

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected
