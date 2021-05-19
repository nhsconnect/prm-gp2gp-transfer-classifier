from prmdata.domain.spine.message import ERROR_SUPPRESSED
from tests.builders.common import a_datetime
from tests.builders.spine import build_parsed_conversation, build_message


def test_extracts_sending_practice_asid():
    conversation = build_parsed_conversation(
        request_started=build_message(to_party_asid="121212121212")
    )

    actual = conversation.sending_practice_asid()

    expected = "121212121212"

    assert actual == expected


def test_extracts_requesting_practice_asid():
    conversation = build_parsed_conversation(
        request_started=build_message(from_party_asid="123456789012")
    )

    actual = conversation.requesting_practice_asid()

    expected = "123456789012"

    assert actual == expected


def test_extracts_requesting_supplier():
    conversation = build_parsed_conversation(request_started=build_message(from_system="EMIS"))

    actual = conversation.requesting_supplier()

    expected = "EMIS"

    assert actual == expected


def test_extracts_sending_supplier():
    conversation = build_parsed_conversation(request_started=build_message(to_system="Vision"))

    actual = conversation.sending_supplier()

    expected = "Vision"

    assert actual == expected


def test_extracts_final_error_codes():
    conversation = build_parsed_conversation(
        request_completed_ack_messages=[
            build_message(error_code=99),
            build_message(error_code=1),
            build_message(error_code=None),
        ]
    )

    actual = conversation.final_error_codes()

    expected = [99, 1, None]

    assert actual == expected


def test_doesnt_extract_error_code_given_pending_request_completed_ack():
    conversation = build_parsed_conversation(request_completed_ack_messages=[])

    actual = conversation.final_error_codes()

    expected = []

    assert actual == expected


def test_extracts_sender_error_code_when_no_sender_error():
    conversation = build_parsed_conversation(request_started_ack=build_message(error_code=None))

    actual = conversation.sender_error()

    expected = None

    assert actual == expected


def test_extracts_sender_error_code_when_sender_error():
    conversation = build_parsed_conversation(request_started_ack=build_message(error_code=10))

    actual = conversation.sender_error()

    expected = 10

    assert actual == expected


def test_extracts_an_intermediate_message_error_code():
    intermediate_messages = [build_message(error_code=20)]
    conversation = build_parsed_conversation(intermediate_messages=intermediate_messages)

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [20]

    assert actual == expected_intermediate_error_codes


def test_intermediate_error_code_is_empty_list_if_no_errors():
    intermediate_messages = [build_message(), build_message(), build_message()]
    conversation = build_parsed_conversation(intermediate_messages=intermediate_messages)

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = []

    assert actual == expected_intermediate_error_codes


def test_extracts_multiple_intermediate_message_error_codes():
    intermediate_messages = [
        build_message(error_code=11),
        build_message(),
        build_message(error_code=10),
    ]
    conversation = build_parsed_conversation(intermediate_messages=intermediate_messages)
    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [11, 10]

    assert actual == expected_intermediate_error_codes


def test_extracts_date_requested_from_request_started_message():
    date_requested = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(time=date_requested),
        request_completed_messages=[build_message()],
        request_completed_ack_messages=[build_message()],
    )

    actual = conversation.date_requested()

    assert actual == date_requested


def test_extracts_date_completed_from_request_completed_ack():
    date_completed = a_datetime()

    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_completed_messages=[build_message()],
        request_completed_ack_messages=[
            build_message(
                time=date_completed,
            )
        ],
    )

    actual = conversation.date_completed()

    assert actual == date_completed


def test_date_completed_is_none_when_request_completed_ack_not_present():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_completed_messages=[build_message()],
        request_completed_ack_messages=[],
    )

    actual = conversation.date_completed()
    expected_date_completed = None

    assert actual == expected_date_completed


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
