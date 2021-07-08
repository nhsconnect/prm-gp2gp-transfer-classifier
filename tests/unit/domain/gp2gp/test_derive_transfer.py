from datetime import datetime, timedelta
from typing import List

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message, FATAL_SENDER_ERROR_CODES
from tests.builders import test_cases
from tests.builders.spine import (
    build_mock_gp2gp_conversation,
)
from prmdata.domain.gp2gp.transfer import (
    TransferStatus,
    derive_transfer,
    TransferFailureReason,
)


def test_extracts_conversation_id():
    conversation = build_mock_gp2gp_conversation(conversation_id="1234")

    actual = derive_transfer(conversation)

    expected_conversation_id = "1234"

    assert actual.conversation_id == expected_conversation_id


@pytest.mark.parametrize(
    "test_case, expected_status, expected_reason",
    [
        (
            test_cases.ehr_integrated_successfully,
            TransferStatus.INTEGRATED_ON_TIME,
            TransferFailureReason.DEFAULT,
        ),
        (
            test_cases.ehr_integrated_late,
            TransferStatus.PROCESS_FAILURE,
            TransferFailureReason.INTEGRATED_LATE,
        ),
        (
            test_cases.ehr_integration_failed,
            TransferStatus.TECHNICAL_FAILURE,
            TransferFailureReason.FINAL_ERROR,
        ),
        (
            test_cases.core_ehr_sent,
            TransferStatus.PROCESS_FAILURE,
            TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
        ),
        (
            test_cases.acknowledged_duplicate_and_waiting_for_integration,
            TransferStatus.PROCESS_FAILURE,
            TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
        ),
        (
            test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs,
            TransferStatus.INTEGRATED_ON_TIME,
            TransferFailureReason.DEFAULT,
        ),
        (
            test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs,
            TransferStatus.INTEGRATED_ON_TIME,
            TransferFailureReason.DEFAULT,
        ),
        (
            test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs,
            TransferStatus.TECHNICAL_FAILURE,
            TransferFailureReason.FINAL_ERROR,
        ),
        (
            test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack,
            TransferStatus.INTEGRATED_ON_TIME,
            TransferFailureReason.DEFAULT,
        ),
        (
            test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack,
            TransferStatus.INTEGRATED_ON_TIME,
            TransferFailureReason.DEFAULT,
        ),
        (
            test_cases.request_made,
            TransferStatus.TECHNICAL_FAILURE,
            TransferFailureReason.REQUEST_NOT_ACKNOWLEDGED,
        ),
        (
            test_cases.request_acknowledged_successfully,
            TransferStatus.TECHNICAL_FAILURE,
            TransferFailureReason.CORE_EHR_NOT_SENT,
        ),
        (
            test_cases.pending_integration_with_large_message_fragments,
            TransferStatus.PROCESS_FAILURE,
            TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
        ),
        (
            test_cases.ehr_suppressed,
            TransferStatus.INTEGRATED_ON_TIME,
            TransferFailureReason.DEFAULT,
        ),
        (
            test_cases.large_message_fragment_failure,
            TransferStatus.PENDING_WITH_ERROR,
            TransferFailureReason.DEFAULT,
        ),
        (
            test_cases.request_acknowledged_with_error,
            TransferStatus.PENDING_WITH_ERROR,
            TransferFailureReason.DEFAULT,
        ),
    ],
)
def test_returns_correct_transfer_outcome(test_case, expected_status, expected_reason):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = derive_transfer(conversation)

    assert actual.transfer_outcome.status == expected_status
    assert actual.transfer_outcome.reason == expected_reason


@pytest.mark.parametrize("fatal_sender_error_code", FATAL_SENDER_ERROR_CODES)
def test_returns_correct_transfer_outcome_if_fatal_sender_error_code_present(
    fatal_sender_error_code,
):
    gp2gp_messages: List[Message] = test_cases.request_acknowledged_with_error(
        error_code=fatal_sender_error_code
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.TECHNICAL_FAILURE
    expected_reason = TransferFailureReason.FATAL_SENDER_ERROR

    assert actual.transfer_outcome.status == expected_status
    assert actual.transfer_outcome.reason == expected_reason


def test_produces_sla_of_successful_conversation():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
        final_acknowledgement_time=datetime(
            year=2020, month=6, day=1, hour=13, minute=52, second=0
        ),
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration


def test_warns_about_conversation_with_negative_sla():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=datetime(year=2021, month=1, day=5),
        final_acknowledgement_time=datetime(year=2021, month=1, day=4),
    )

    with pytest.warns(RuntimeWarning):
        derive_transfer(conversation)


def test_negative_sla_duration_clamped_to_zero():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=datetime(year=2021, month=1, day=5),
        final_acknowledgement_time=datetime(year=2021, month=1, day=4),
    )

    expected_sla_duration = timedelta(0)

    actual = derive_transfer(conversation)

    assert actual.sla_duration == expected_sla_duration


def test_produces_no_sla_given_no_request_completed_time():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=None,
        final_acknowledgement_time=None,
    )
    actual = derive_transfer(conversation)

    expected_sla_duration = None

    assert actual.sla_duration == expected_sla_duration


def test_produces_no_sla_given_no_final_acknowledgement_time():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=datetime(year=2021, month=1, day=5),
        final_acknowledgement_time=None,
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = None

    assert actual.sla_duration == expected_sla_duration


def test_produces_no_sla_given_acks_with_only_duplicate_error():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.acknowledged_duplicate_and_waiting_for_integration()
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = None
    expected_date_completed = None

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == expected_date_completed


def test_produces_sla_given_integration_with_conflicting_acks_and_duplicate_ehrs():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )

    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs(
            request_completed_time=datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        )
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_given_suppression_with_conflicting_acks_and_duplicate_ehrs():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs(
            request_completed_time=datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        )
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_given_failure_with_conflicting_acks_and_duplicate_ehrs():
    failed_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs(
            request_completed_time=datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=failed_acknowledgement_datetime,
        )
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == failed_acknowledgement_datetime


def test_produces_sla_given_integration_with_conflicting_duplicate_and_error_acks():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )

    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack(
            request_completed_time=datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        )
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = timedelta(hours=4, minutes=0, seconds=1)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_given_suppression_with_conflicting_duplicate_and_error_acks():
    successful_acknowledgement_datetime = datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )

    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack(
            request_completed_time=datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        )
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = timedelta(hours=4, minutes=0, seconds=1)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime
