from datetime import datetime, timedelta

from gp2gp.service.transformers import derive_transfer
from tests.builders.spine import build_message, build_parsed_conversation


def test_derive_transfer_extracts_conversation_id():
    conversation = build_parsed_conversation(id="1234")

    actual = derive_transfer(conversation)

    expected_conversation_id = "1234"

    assert actual.conversation_id == expected_conversation_id


def test_derive_transfer_produces_sla_of_successful_conversation():
    conversation = build_parsed_conversation(
        request_completed=build_message(
            time=datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
        ),
        request_completed_ack=build_message(
            time=datetime(year=2020, month=6, day=1, hour=13, minute=52, second=0), error_code=None
        ),
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration


def test_derive_transfer_produces_no_sla_given_pending_ehr_completed():
    conversation = build_parsed_conversation(
        request_started=build_message(), request_completed=None, request_completed_ack=None,
    )
    actual = derive_transfer(conversation)

    expected_sla_duration = None

    assert actual.sla_duration == expected_sla_duration


def test_derive_transfer_produces_no_sla_given_pending_request_completed_ack():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_completed=build_message(),
        request_completed_ack=None,
    )
    actual = derive_transfer(conversation)

    expected_sla_duration = None

    assert actual.sla_duration == expected_sla_duration


def test_derive_transfer_extracts_requesting_practice_ods():
    conversation = build_parsed_conversation(request_started=build_message(from_party_ods="A12345"))

    actual = derive_transfer(conversation)

    expected_ods = "A12345"
    assert actual.requesting_practice_ods == expected_ods


def test_derive_transfer_extracts_sending_practice_ods():
    conversation = build_parsed_conversation(request_started=build_message(to_party_ods="A12377"))

    actual = derive_transfer(conversation)

    expected_ods = "A12377"

    assert actual.sending_practice_ods == expected_ods


def test_derive_transfer_extracts_error_code():
    conversation = build_parsed_conversation(request_completed_ack=build_message(error_code=99))

    actual = derive_transfer(conversation)

    expected_error = 99

    assert actual.error_code == expected_error


def test_derive_transfer_doesnt_extract_error_code_given_pending_request_completed_ack():
    conversation = build_parsed_conversation(request_completed_ack=None)

    actual = derive_transfer(conversation)

    expected_error = None

    assert actual.error_code == expected_error


def test_derive_transfer_flags_pending_request_completed_as_pending():
    conversation = build_parsed_conversation(
        request_started=build_message(), request_completed=None, request_completed_ack=None
    )

    actual = derive_transfer(conversation)

    expected_pending = True

    assert actual.pending == expected_pending


def test_derive_transfer_flags_pending_request_completed_ack_as_pending():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_completed=build_message(),
        request_completed_ack=None,
    )

    actual = derive_transfer(conversation)

    expected_pending = True

    assert actual.pending == expected_pending


def test_derive_transfer_flags_completed_conversation_as_not_pending():
    conversation = build_parsed_conversation(
        request_started=build_message(),
        request_completed=build_message(),
        request_completed_ack=build_message(),
    )

    actual = derive_transfer(conversation)

    expected_pending = False

    assert actual.pending == expected_pending
