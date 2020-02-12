from datetime import datetime, timedelta

from gp2gp.service.transformers import derive_transfers
from tests.builders.spine import build_parsed_conversation, build_message


def _assert_attributes(attr_name, actual, expected):
    assert [getattr(i, attr_name) for i in actual] == expected


def test_derive_transfers_extracts_conversation_id():
    conversations = [build_parsed_conversation(id="1234")]

    actual = derive_transfers(conversations)

    expected_conversation_ids = ["1234"]

    _assert_attributes("conversation_id", actual, expected_conversation_ids)


def test_derive_transfers_produces_sla_of_successful_conversation():
    conversations = [
        build_parsed_conversation(
            request_completed=build_message(
                time=datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
            ),
            request_completed_ack=build_message(
                time=datetime(year=2020, month=6, day=1, hour=13, minute=52, second=0),
                error_code=None,
            ),
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [timedelta(hours=1, minutes=10)]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_derive_transfers_produces_no_sla_given_pending_ehr_completed():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(), request_completed=None, request_completed_ack=None,
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_derive_transfers_produces_no_sla_given_pending_request_completed_ack():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed=build_message(),
            request_completed_ack=None,
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_derive_transfers_extracts_requesting_practice_ods_code():
    conversations = [
        build_parsed_conversation(request_started=build_message(from_party_ods_code="A12345"))
    ]

    actual = derive_transfers(conversations)

    expected_ods_codes = ["A12345"]

    _assert_attributes("requesting_practice_ods_code", actual, expected_ods_codes)


def test_derive_transfers_extracts_sending_practice_ods_code():
    conversations = [
        build_parsed_conversation(request_started=build_message(to_party_ods_code="A12377"))
    ]

    actual = derive_transfers(conversations)

    expected_ods_codes = ["A12377"]

    _assert_attributes("sending_practice_ods_code", actual, expected_ods_codes)


def test_derive_transfers_extracts_error_code():
    conversations = [build_parsed_conversation(request_completed_ack=build_message(error_code=99))]

    actual = derive_transfers(conversations)

    expected_errors = [99]

    _assert_attributes("error_code", actual, expected_errors)


def test_derive_transfers_doesnt_extract_error_code_given_pending_request_completed_ack():
    conversations = [build_parsed_conversation(request_completed_ack=None)]

    actual = derive_transfers(conversations)

    expected_errors = [None]

    _assert_attributes("error_code", actual, expected_errors)


def test_derive_transfers_flags_pending_request_completed_as_pending():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(), request_completed=None, request_completed_ack=None
        )
    ]

    actual = derive_transfers(conversations)

    expected_pending_statuses = [True]

    _assert_attributes("pending", actual, expected_pending_statuses)


def test_derive_transfers_flags_pending_request_completed_ack_as_pending():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed=build_message(),
            request_completed_ack=None,
        )
    ]

    actual = derive_transfers(conversations)

    expected_pending_statuses = [True]

    _assert_attributes("pending", actual, expected_pending_statuses)


def test_derive_transfers_flags_completed_conversation_as_not_pending():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed=build_message(),
            request_completed_ack=build_message(),
        )
    ]

    actual = derive_transfers(conversations)

    expected_pending_statuses = [False]

    _assert_attributes("pending", actual, expected_pending_statuses)


def test_derive_transfers_extracts_conversation_ids_for_conversations():
    conversations = [
        build_parsed_conversation(id="1234"),
        build_parsed_conversation(id="3456"),
        build_parsed_conversation(id="5678"),
    ]

    actual = derive_transfers(conversations)

    expected_conversation_ids = ["1234", "3456", "5678"]

    _assert_attributes("conversation_id", actual, expected_conversation_ids)
