from datetime import datetime, timedelta
from typing import List

from gp2gp.service.transformers import derive_transfers
from tests.builders.spine import build_parsed_conversation, build_message
from gp2gp.service.models import Transfer, TransferStatus, ERROR_SUPPRESSED


def _assert_attributes(attr_name: str, actual: List[Transfer], expected: List):
    assert [getattr(i, attr_name) for i in actual] == expected


def test_extracts_conversation_id():
    conversations = [build_parsed_conversation(id="1234")]

    actual = derive_transfers(conversations)

    expected_conversation_ids = ["1234"]

    _assert_attributes("conversation_id", actual, expected_conversation_ids)


def test_produces_sla_of_successful_conversation():
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


def test_produces_no_sla_given_pending_ehr_completed():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed=None,
            request_completed_ack=None,
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_no_sla_given_pending_request_completed_ack():
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


def test_extracts_requesting_practice_ods_code():
    conversations = [
        build_parsed_conversation(request_started=build_message(from_party_ods_code="A12345"))
    ]

    actual = derive_transfers(conversations)

    expected_ods_codes = ["A12345"]

    _assert_attributes("requesting_practice_ods_code", actual, expected_ods_codes)


def test_extracts_sending_practice_ods_code():
    conversations = [
        build_parsed_conversation(request_started=build_message(to_party_ods_code="A12377"))
    ]

    actual = derive_transfers(conversations)

    expected_ods_codes = ["A12377"]

    _assert_attributes("sending_practice_ods_code", actual, expected_ods_codes)


def test_extracts_error_code():
    conversations = [build_parsed_conversation(request_completed_ack=build_message(error_code=99))]

    actual = derive_transfers(conversations)

    expected_errors = [99]

    _assert_attributes("final_error_code", actual, expected_errors)


def test_doesnt_extract_error_code_given_pending_request_completed_ack():
    conversations = [build_parsed_conversation(request_completed_ack=None)]

    actual = derive_transfers(conversations)

    expected_errors = [None]

    _assert_attributes("final_error_code", actual, expected_errors)


def test_flags_pending_request_completed_as_pending():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(), request_completed=None, request_completed_ack=None
        )
    ]

    actual = derive_transfers(conversations)

    expected_pending_statuses = [True]

    _assert_attributes("pending", actual, expected_pending_statuses)


def test_flags_pending_request_completed_ack_as_pending():
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


def test_flags_completed_conversation_as_not_pending():
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


def test_extracts_conversation_ids_for_conversations():
    conversations = [
        build_parsed_conversation(id="1234"),
        build_parsed_conversation(id="3456"),
        build_parsed_conversation(id="5678"),
    ]

    actual = derive_transfers(conversations)

    expected_conversation_ids = ["1234", "3456", "5678"]

    _assert_attributes("conversation_id", actual, expected_conversation_ids)


def test_intermediate_error_code_is_empty_list_if_no_errors():
    intermediate_messages = [build_message(), build_message(), build_message()]
    conversations = [build_parsed_conversation(intermediate_messages=intermediate_messages)]

    actual = derive_transfers(conversations)

    expected_intermediate_error_codes = [[]]

    _assert_attributes("intermediate_error_codes", actual, expected_intermediate_error_codes)


def test_extracts_an_intermediate_message_error_code():
    intermediate_messages = [build_message(error_code=20)]
    conversations = [build_parsed_conversation(intermediate_messages=intermediate_messages)]

    actual = derive_transfers(conversations)

    expected_intermediate_error_codes = [[20]]

    _assert_attributes("intermediate_error_codes", actual, expected_intermediate_error_codes)


def test_extracts_multiple_intermediate_message_error_codes():
    intermediate_messages = [
        build_message(error_code=11),
        build_message(),
        build_message(error_code=10),
    ]
    conversations = [build_parsed_conversation(intermediate_messages=intermediate_messages)]

    actual = derive_transfers(conversations)

    expected_intermediate_error_codes = [[11, 10]]

    _assert_attributes("intermediate_error_codes", actual, expected_intermediate_error_codes)


def test_has_requested_status():
    conversations = [
        build_parsed_conversation(request_started=build_message(), request_completed_ack=None)
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.REQUESTED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_if_no_error_in_final_ack():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed=build_message(),
            request_completed_ack=build_message(error_code=None),
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_if_error_is_supressed():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed=build_message(),
            request_completed_ack=build_message(error_code=ERROR_SUPPRESSED),
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)
