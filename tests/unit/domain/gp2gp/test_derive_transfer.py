from datetime import datetime, timedelta

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases
from tests.builders.common import a_datetime
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


def test_produces_no_sla_and_pending_status_given_acks_with_only_duplicate_error():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.acknowledged_duplicate_and_waiting_for_integration()
    )

    actual = derive_transfer(conversation)

    expected_sla_duration = None
    expected_status = TransferStatus.PENDING
    expected_date_completed = None

    assert actual.sla_duration == expected_sla_duration
    assert actual.transfer_outcome.status == expected_status
    assert actual.date_completed == expected_date_completed


def test_produces_sla_and_status_given_integration_with_conflicting_acks_and_duplicate_ehrs():
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
    expected_status = TransferStatus.INTEGRATED_ON_TIME

    assert actual.sla_duration == expected_sla_duration
    assert actual.transfer_outcome.status == expected_status
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_and_status_given_suppression_with_conflicting_acks_and_duplicate_ehrs():
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
    expected_status = TransferStatus.INTEGRATED_ON_TIME

    assert actual.sla_duration == expected_sla_duration
    assert actual.transfer_outcome.status == expected_status
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_and_status_given_failure_with_conflicting_acks_and_duplicate_ehrs():
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
    expected_status = TransferStatus.FAILED

    assert actual.sla_duration == expected_sla_duration
    assert actual.transfer_outcome.status == expected_status
    assert actual.date_completed == failed_acknowledgement_datetime


def test_produces_sla_and_status_given_integration_with_conflicting_duplicate_and_error_acks():
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
    expected_status = TransferStatus.INTEGRATED_ON_TIME

    assert actual.sla_duration == expected_sla_duration
    assert actual.transfer_outcome.status == expected_status
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_and_status_given_suppression_with_conflicting_duplicate_and_error_acks():
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
    expected_status = TransferStatus.INTEGRATED_ON_TIME

    assert actual.sla_duration == expected_sla_duration
    assert actual.transfer_outcome.status == expected_status
    assert actual.date_completed == successful_acknowledgement_datetime


def test_has_pending_status_if_no_final_ack():
    conversation = Gp2gpConversation.from_messages(messages=test_cases.core_ehr_sent())

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.PENDING

    assert actual.transfer_outcome.status == expected_status


def test_has_pending_status_if_no_request_completed_message():
    conversation = Gp2gpConversation.from_messages(messages=test_cases.request_made())

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.PENDING

    assert actual.transfer_outcome.status == expected_status


def test_has_pending_status_if_no_final_ack_and_no_intermediate_error():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.pending_integration_with_large_message_fragments()
    )

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.PENDING

    assert actual.transfer_outcome.status == expected_status


def test_has_integrated_status_if_no_error_in_final_ack():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_integrated_successfully()
    )

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.INTEGRATED_ON_TIME

    assert actual.transfer_outcome.status == expected_status


def test_has_integrated_status_if_error_is_suppressed():
    conversation = Gp2gpConversation.from_messages(messages=test_cases.ehr_suppressed())

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.INTEGRATED_ON_TIME

    assert actual.transfer_outcome.status == expected_status


def test_has_failed_status_if_error_in_final_ack():
    conversation = Gp2gpConversation.from_messages(messages=test_cases.ehr_integration_failed())

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.FAILED

    assert actual.transfer_outcome.status == expected_status


def test_has_pending_with_error_status_if_error_in_intermediate_message():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.large_message_fragment_failure()
    )

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.PENDING_WITH_ERROR

    assert actual.transfer_outcome.status == expected_status


def test_has_pending_with_error_status_if_error_in_request_acknowledgement():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.request_acknowledged_with_error()
    )

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.PENDING_WITH_ERROR

    assert actual.transfer_outcome.status == expected_status


def test_has_integrated_on_time_status_if_ehr_integrated_successfully_within_8_days():
    request_completed_time = a_datetime(year=2021, month=5, day=1)
    ehr_acknowledge_time = a_datetime(year=2021, month=5, day=5)

    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_integrated_successfully(
            ehr_acknowledge_time=ehr_acknowledge_time, request_completed_time=request_completed_time
        )
    )

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.INTEGRATED_ON_TIME

    assert actual.transfer_outcome.status == expected_status


def test_has_process_failure_status_with_integrated_late_reason_if_ehr_integrated_successfully_beyond_8_days():
    request_completed_time = a_datetime(year=2021, month=5, day=1)
    ehr_acknowledge_time = a_datetime(year=2021, month=5, day=10)

    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_integrated_successfully(
            ehr_acknowledge_time=ehr_acknowledge_time, request_completed_time=request_completed_time
        )
    )

    actual = derive_transfer(conversation)

    expected_status = TransferStatus.PROCESS_FAILURE
    expected_reason = TransferFailureReason.INTEGRATED_LATE

    assert actual.transfer_outcome.status == expected_status
    assert actual.transfer_outcome.reason == expected_reason
