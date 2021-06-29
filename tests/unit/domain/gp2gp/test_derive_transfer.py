from datetime import datetime, timedelta
from typing import List, Iterable

import pytest

from prmdata.domain.spine.message import ERROR_SUPPRESSED, DUPLICATE_ERROR
from tests.builders.spine import (
    build_gp2gp_conversation,
    build_message,
    build_mock_gp2gp_conversation,
)
from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    derive_transfers,
)


def _assert_attributes(attr_name: str, actual: Iterable[Transfer], expected: List):
    assert [getattr(i, attr_name) for i in actual] == expected


def test_extracts_conversation_id():
    conversations = [build_mock_gp2gp_conversation(conversation_id="1234")]

    actual = derive_transfers(conversations)

    expected_conversation_ids = ["1234"]

    _assert_attributes("conversation_id", actual, expected_conversation_ids)


def test_extracts_conversation_ids_for_conversations():
    conversations = [
        build_mock_gp2gp_conversation(conversation_id="1234"),
        build_mock_gp2gp_conversation(conversation_id="3456"),
        build_mock_gp2gp_conversation(conversation_id="5678"),
    ]

    actual = derive_transfers(conversations)

    expected_conversation_ids = ["1234", "3456", "5678"]

    _assert_attributes("conversation_id", actual, expected_conversation_ids)


def test_produces_sla_of_successful_conversation():
    conversations = [
        build_mock_gp2gp_conversation(
            request_completed_time=datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            final_acknowledgement_time=datetime(
                year=2020, month=6, day=1, hour=13, minute=52, second=0
            ),
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [timedelta(hours=1, minutes=10)]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_sla_and_integrated_status_given_acks_with_duplicate_error_and_without_error():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversations = [
        build_gp2gp_conversation(
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
                    time=successful_acknowledgement_datetime,
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

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=1, minutes=10)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_produces_sla_and_integrated_status_given_acks_with_duplicate_error_and_suppressed_error():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversations = [
        build_gp2gp_conversation(
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
                    time=successful_acknowledgement_datetime,
                    message_ref="aaa",
                    error_code=ERROR_SUPPRESSED,
                ),
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=16, minute=42, second=1),
                    message_ref="ddd",
                    error_code=DUPLICATE_ERROR,
                ),
            ],
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=1, minutes=10)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_produces_no_sla_and_pending_status_given_acks_with_only_duplicate_error():
    conversations = [
        build_gp2gp_conversation(
            request_started=build_message(),
            request_completed_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=4, minute=42, second=0),
                    guid="ddd",
                )
            ],
            request_completed_ack_messages=[
                build_message(
                    time=datetime(year=2020, month=6, day=1, hour=4, minute=43, second=0),
                    message_ref="ddd",
                    error_code=DUPLICATE_ERROR,
                )
            ],
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [None]
    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [None])


def test_produces_sla_and_failed_status_given_acks_with_duplicate_error_and_other_error():
    failed_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversations = [
        build_gp2gp_conversation(
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
                    time=failed_acknowledgement_datetime,
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

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=1, minutes=10)]
    expected_statuses = [TransferStatus.FAILED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [failed_acknowledgement_datetime])


def test_produces_sla_and_integrated_status_given_acks_with_duplicate_no_error_and_other_error():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )
    conversations = [
        build_gp2gp_conversation(
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
                    error_code=11,
                ),
                build_message(
                    time=successful_acknowledgement_datetime,
                    message_ref="aaa",
                    error_code=None,
                ),
            ],
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=4, minutes=0, seconds=1)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_produces_sla_and_integrated_status_given_acks_with_duplicate_suppressed_and_other_errors():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )
    conversations = [
        build_gp2gp_conversation(
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
                    error_code=11,
                ),
                build_message(
                    time=successful_acknowledgement_datetime,
                    message_ref="aaa",
                    error_code=ERROR_SUPPRESSED,
                ),
            ],
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=4, minutes=0, seconds=1)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_produces_no_sla_given_pending_ehr_completed():
    conversations = [
        build_gp2gp_conversation(
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
        build_gp2gp_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message()],
            request_completed_ack_messages=[],
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_has_pending_status_if_no_final_ack():
    conversations = [
        build_gp2gp_conversation(request_started=build_message(), request_completed_ack_messages=[])
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_status_if_no_request_completed_message():
    conversations = [
        build_gp2gp_conversation(
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
        build_gp2gp_conversation(
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
        build_gp2gp_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message(guid="abc")],
            request_completed_ack_messages=[build_message(error_code=None, message_ref="abc")],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_if_error_is_suppressed():
    conversations = [
        build_gp2gp_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message(guid="abc")],
            request_completed_ack_messages=[
                build_message(error_code=ERROR_SUPPRESSED, message_ref="abc")
            ],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_failed_status_if_error_in_final_ack():
    conversations = [
        build_gp2gp_conversation(
            request_started=build_message(),
            request_completed_messages=[build_message(guid="abc")],
            request_completed_ack_messages=[build_message(error_code=30, message_ref="abc")],
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.FAILED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_with_error_status_if_error_in_intermediate_message():
    conversations = [
        build_gp2gp_conversation(
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
        build_gp2gp_conversation(
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


def test_negative_sla_duration_clamped_to_zero():
    conversations = [
        build_gp2gp_conversation(
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
        build_gp2gp_conversation(
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
