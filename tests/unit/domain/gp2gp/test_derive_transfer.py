from datetime import datetime, timedelta
from typing import List, Iterable

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases
from tests.builders.spine import (
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


def test_warns_about_conversation_with_negative_sla():
    conversations = [
        build_mock_gp2gp_conversation(
            request_completed_time=datetime(year=2021, month=1, day=5),
            final_acknowledgement_time=datetime(year=2021, month=1, day=4),
        )
    ]

    with pytest.warns(RuntimeWarning):
        list(derive_transfers(conversations))


def test_negative_sla_duration_clamped_to_zero():
    conversations = [
        build_mock_gp2gp_conversation(
            request_completed_time=datetime(year=2021, month=1, day=5),
            final_acknowledgement_time=datetime(year=2021, month=1, day=4),
        )
    ]

    expected_sla = timedelta(0)

    actual = derive_transfers(conversations)

    _assert_attributes("sla_duration", actual, [expected_sla])


def test_produces_no_sla_given_no_request_completed_time():
    conversations = [
        build_mock_gp2gp_conversation(
            request_completed_time=None,
            final_acknowledgement_time=None,
        )
    ]
    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_no_sla_given_no_final_acknowledgement_time():
    conversations = [
        build_mock_gp2gp_conversation(
            request_completed_time=datetime(year=2021, month=1, day=5),
            final_acknowledgement_time=None,
        )
    ]

    actual = derive_transfers(conversations)

    expected_sla_durations = [None]

    _assert_attributes("sla_duration", actual, expected_sla_durations)


def test_produces_no_sla_and_pending_status_given_acks_with_only_duplicate_error():
    conversations = [
        Gp2gpConversation.from_messages(
            messages=test_cases.acknowledged_duplicate_and_waiting_for_integration()
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [None]
    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [None])


def test_produces_sla_and_status_given_integration_with_conflicting_acks_and_duplicate_ehrs():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )

    conversations = [
        Gp2gpConversation.from_messages(
            messages=test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs(
                request_completed_time=datetime(
                    year=2020, month=6, day=1, hour=12, minute=42, second=0
                ),
                ehr_acknowledge_time=successful_acknowledgement_datetime,
            )
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=1, minutes=10)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_produces_sla_and_status_given_suppression_with_conflicting_acks_and_duplicate_ehrs():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversations = [
        Gp2gpConversation.from_messages(
            messages=test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs(
                request_completed_time=datetime(
                    year=2020, month=6, day=1, hour=12, minute=42, second=0
                ),
                ehr_acknowledge_time=successful_acknowledgement_datetime,
            )
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=1, minutes=10)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_produces_sla_and_status_given_failure_with_conflicting_acks_and_duplicate_ehrs():
    failed_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversations = [
        Gp2gpConversation.from_messages(
            messages=test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs(
                request_completed_time=datetime(
                    year=2020, month=6, day=1, hour=12, minute=42, second=0
                ),
                ehr_acknowledge_time=failed_acknowledgement_datetime,
            )
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=1, minutes=10)]
    expected_statuses = [TransferStatus.FAILED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [failed_acknowledgement_datetime])


def test_produces_sla_and_status_given_integration_with_conflicting_duplicate_and_error_acks():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )

    conversations = [
        Gp2gpConversation.from_messages(
            messages=test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack(
                request_completed_time=datetime(
                    year=2020, month=6, day=1, hour=12, minute=42, second=0
                ),
                ehr_acknowledge_time=successful_acknowledgement_datetime,
            )
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=4, minutes=0, seconds=1)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_produces_sla_and_status_given_suppression_with_conflicting_duplicate_and_error_acks():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )

    conversations = [
        Gp2gpConversation.from_messages(
            messages=test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack(
                request_completed_time=datetime(
                    year=2020, month=6, day=1, hour=12, minute=42, second=0
                ),
                ehr_acknowledge_time=successful_acknowledgement_datetime,
            )
        )
    ]

    actual = list(derive_transfers(conversations))

    expected_sla_durations = [timedelta(hours=4, minutes=0, seconds=1)]
    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("sla_duration", actual, expected_sla_durations)
    _assert_attributes("status", actual, expected_statuses)
    _assert_attributes("date_completed", actual, [successful_acknowledgement_datetime])


def test_has_pending_status_if_no_final_ack():
    conversations = [Gp2gpConversation.from_messages(messages=test_cases.core_ehr_sent())]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_status_if_no_request_completed_message():
    conversations = [Gp2gpConversation.from_messages(messages=test_cases.request_made())]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_status_if_no_final_ack_and_no_intermediate_error():
    conversations = [
        Gp2gpConversation.from_messages(
            messages=test_cases.pending_integration_with_large_message_fragments()
        )
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_if_no_error_in_final_ack():
    conversations = [
        Gp2gpConversation.from_messages(messages=test_cases.ehr_integrated_successfully())
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_integrated_status_if_error_is_suppressed():
    conversations = [Gp2gpConversation.from_messages(messages=test_cases.ehr_suppressed())]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.INTEGRATED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_failed_status_if_error_in_final_ack():
    conversations = [Gp2gpConversation.from_messages(messages=test_cases.ehr_integration_failed())]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.FAILED]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_with_error_status_if_error_in_intermediate_message():
    conversations = [
        Gp2gpConversation.from_messages(messages=test_cases.large_message_fragment_failure())
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING_WITH_ERROR]

    _assert_attributes("status", actual, expected_statuses)


def test_has_pending_with_error_status_if_error_in_request_acknowledgement():
    conversations = [
        Gp2gpConversation.from_messages(messages=test_cases.request_acknowledged_with_error())
    ]

    actual = derive_transfers(conversations)

    expected_statuses = [TransferStatus.PENDING_WITH_ERROR]

    _assert_attributes("status", actual, expected_statuses)
