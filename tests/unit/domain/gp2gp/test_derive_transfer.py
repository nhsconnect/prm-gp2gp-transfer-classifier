from datetime import datetime, timedelta
from typing import List, Iterator

import pytest
from tests.builders.spine import build_parsed_conversation, build_message
from tests.builders.common import a_datetime
from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    ERROR_SUPPRESSED,
    DUPLICATE_ERROR,
    derive_transfers,
)


def _assert_attributes(attr_name: str, actual: Iterator[Transfer], expected: List):
    assert [getattr(i, attr_name) for i in actual] == expected


def test_extracts_conversation_id():
    conversations = [build_parsed_conversation(id="1234")]

    actual = derive_transfers(conversations)

    expected_conversation_ids = ["1234"]

    _assert_attributes("conversation_id", actual, expected_conversation_ids)


def test_produces_sla_of_successful_conversation():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
                    guid="abc",
                )
            ],
            request_completed_ack_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=13, minute=52, second=0),
                    message_ref="abc",
                    error_code=None,
                )
            ],
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [timedelta(hours=1, minutes=10)]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_sla_of_successful_conversation_given_multiple_final_acks():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=4, minute=42, second=0),
                    guid="ddd",
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
                    guid="aaa",
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=16, minute=42, second=0),
                    guid="xxx",
                ),
            ],
            request_completed_ack_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=13, minute=13, second=0),
                    message_ref="aaa",
                    error_code=DUPLICATE_ERROR,
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=13, minute=52, second=0),
                    message_ref="aaa",
                    error_code=None,
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=16, minute=42, second=1),
                    message_ref="ddd",
                    error_code=DUPLICATE_ERROR,
                ),
            ],
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [timedelta(hours=1, minutes=10)]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_sla_of_failed_conversation_given_multiple_final_acks():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=4, minute=42, second=0),
                    guid="ddd",
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
                    guid="aaa",
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=16, minute=42, second=0),
                    guid="xxx",
                ),
            ],
            request_completed_ack_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=13, minute=13, second=0),
                    message_ref="xxx",
                    error_code=DUPLICATE_ERROR,
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=13, minute=52, second=0),
                    message_ref="aaa",
                    error_code=99,
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=16, minute=42, second=1),
                    message_ref="ddd",
                    error_code=DUPLICATE_ERROR,
                ),
            ],
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [timedelta(hours=1, minutes=10)]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_no_sla_given_pending_ehr_completed():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_no_sla_given_pending_request_completed_ack():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_extracts_requesting_practice_asid():
    conversations = [
        build_parsed_conversation(request_started=build_message(from_party_asid="123456789012"))
    ]

    actual = derive_transfers(conversations)

    expected_asids = ["123456789012"]

    _assert_attributes("requesting_practice_asid", actual, expected_asids)


def test_extracts_sending_practice_asid():
    conversations = [
        build_parsed_conversation(request_started=build_message(to_party_asid="121212121212"))
    ]

    actual = derive_transfers(conversations)

    expected_asids = ["121212121212"]

    _assert_attributes("sending_practice_asid", actual, expected_asids)


def test_extracts_final_error_codes():
    conversations = [
        build_parsed_conversation(
            request_completed_ack_messages=[
                build_message(error_code=99),
                build_message(error_code=1),
                build_message(error_code=None),
            ]
        )
    ]

    actual = derive_transfers(conversations)

    expected_errors = [[99, 1, None]]

    _assert_attributes("final_error_codes", actual, expected_errors)


def test_extracts_sender_error_code_when_no_sender_errror():
    conversations = [build_parsed_conversation(request_started_ack=build_message(error_code=None))]

    actual = derive_transfers(conversations)

    expected_errors = [None]

    _assert_attributes("sender_error_code", actual, expected_errors)


def test_extracts_sender_error_code_when_sender_error():
    conversations = [build_parsed_conversation(request_started_ack=build_message(error_code=10))]

    actual = derive_transfers(conversations)

    expected_errors = [10]

    _assert_attributes("sender_error_code", actual, expected_errors)


def test_doesnt_extract_error_code_given_pending_request_completed_ack():
    conversations = [build_parsed_conversation(request_completed_ack_messages=[])]

    actual = derive_transfers(conversations)

    expected_errors = [[]]

    _assert_attributes("final_error_codes", actual, expected_errors)


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


def test_has_pending_status_if_no_final_ack():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(), request_completed_ack_messages=[]
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_status_if_no_request_completed_message():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_status_if_no_final_ack_and_no_intermediate_error():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            intermediate_messages=[build_message(error_code=None)],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_if_no_error_in_final_ack():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[build_message(error_code=None)],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_if_error_is_supressed():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[build_message(error_code=ERROR_SUPPRESSED)],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_given_one_ack_with_duplicate_error_and_another_without_error():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[
                build_message(error_code=DUPLICATE_ERROR),
                build_message(error_code=None),
            ],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_failed_status_if_error_in_final_ack():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[build_message(error_code=30)],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.FAILED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_with_error_status_if_error_in_intermediate_message():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            intermediate_messages=[build_message(error_code=30)],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING_WITH_ERROR]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_with_error_status_if_error_in_request_acknowledgement():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_started_ack=build_message(error_code=10),
            request_completed_messages=[build_message()],
            intermediate_messages=[],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING_WITH_ERROR]

    _assert_attributes("status", actual, expected_statuses)


def test_extracts_date_requested_from_request_started_message():
    date_requested = a_datetime()

    conversations = [
        build_parsed_conversation(
            request_started=build_message(time=date_requested),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[build_message()],
        )
    ]

    actual = derive_transfers(conversations)

    _assert_attributes("date_requested", actual, [date_requested])


def test_extracts_date_completed_from_request_completed_ack():
    date_completed = a_datetime()

    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[
                build_message(
                    time=date_completed,
                )
            ],
        )
    ]

    actual = derive_transfers(conversations)

    _assert_attributes("date_completed", actual, [date_completed])


def test_date_completed_is_none_when_request_completed_ack_not_present():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    _assert_attributes("date_completed", actual, [None])


def test_extracts_requesting_supplier():
    conversations = [build_parsed_conversation(request_started=build_message(from_system="EMIS"))]

    actual = derive_transfers(conversations)

    expected_supplier_name = ["EMIS"]

    _assert_attributes("requesting_supplier", actual, expected_supplier_name)


def test_extracts_sending_supplier():
    conversations = [build_parsed_conversation(request_started=build_message(to_system="Vision"))]

    actual = derive_transfers(conversations)

    expected_supplier_name = ["Vision"]

    _assert_attributes("sending_supplier", actual, expected_supplier_name)


def test_negative_sla_duration_clamped_to_zero():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[
                build_message(time=datetime(year=2021, month=1, day=5), guid="abc")
            ],
            request_completed_ack_messages=[
                build_message(time=datetime(year=2021, month=1, day=4), message_ref="abc")
            ],
        )
    ]

    expected_sla = timedelta(0)

    actual = derive_transfers(conversations)

    _assert_attributes("sla_duration", actual, [expected_sla])


def test_warns_about_conversation_with_negative_sla():
    conversations = [
        build_parsed_conversation(
            request_started=build_message(),
            request_completed_messages=[
                build_message(time=datetime(year=2021, month=1, day=5), guid="abc")
            ],
            request_completed_ack_messages=[
                build_message(time=datetime(year=2021, month=1, day=4), message_ref="abc")
            ],
        )
    ]

    with pytest.warns(RuntimeWarning):
        list(derive_transfers(conversations))
