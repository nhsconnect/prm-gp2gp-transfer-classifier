from datetime import timedelta
from random import choice
from typing import List

import pytest

from prmdata.domain.gp2gp.transfer_outcome import (
    TransferFailureReason,
    TransferOutcome,
    TransferStatus,
)
from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import FATAL_SENDER_ERROR_CODES, Message
from tests.builders import test_cases
from tests.builders.common import a_duration
from tests.builders.spine import build_mock_gp2gp_conversation
from tests.unit.domain.gp2gp.transfer_service.test_derive_transfer import (
    mock_gp2gp_conversation_observability_probe,
)


@pytest.mark.parametrize(
    "test_case, expected_reason",
    [
        (
            test_cases.ehr_integration_failed,
            TransferFailureReason.FINAL_ERROR,
        ),
        (
            test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs,
            TransferFailureReason.FINAL_ERROR,
        ),
        (
            test_cases.request_made,
            TransferFailureReason.REQUEST_NOT_ACKNOWLEDGED,
        ),
        (
            test_cases.request_acknowledged_successfully,
            TransferFailureReason.CORE_EHR_NOT_SENT,
        ),
        (
            test_cases.copc_continue_sent,
            TransferFailureReason.COPC_NOT_SENT,
        ),
        (
            test_cases.pending_integration_with_copc_fragments,
            TransferFailureReason.COPC_NOT_ACKNOWLEDGED,
        ),
        (
            test_cases.request_acknowledged_with_error,
            TransferFailureReason.CORE_EHR_NOT_SENT,
        ),
        (
            test_cases.copc_fragment_failure_and_missing_copc_fragment_ack,
            TransferFailureReason.COPC_NOT_ACKNOWLEDGED,
        ),
    ],
)
def test_returns_transfer_status_technical_failure_with_reason(test_case, expected_reason):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = TransferOutcome.from_gp2gp_conversation(conversation, a_duration())

    assert actual.status == TransferStatus.TECHNICAL_FAILURE
    assert actual.failure_reason == expected_reason


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.ehr_integrated_successfully,
        test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack,
        test_cases.ehr_suppressed,
        test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack,
    ],
)
def test_returns_transfer_status_integrated_on_time(test_case):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = TransferOutcome.from_gp2gp_conversation(conversation, timedelta(days=1))

    assert actual.status == TransferStatus.INTEGRATED_ON_TIME
    assert actual.failure_reason is None


@pytest.mark.parametrize(
    "test_case, expected_reason",
    [
        (
            test_cases.ehr_integrated_late,
            TransferFailureReason.INTEGRATED_LATE,
        ),
        (
            test_cases.core_ehr_sent,
            TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
        ),
        (
            test_cases.acknowledged_duplicate_and_waiting_for_integration,
            TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
        ),
        (
            test_cases.pending_integration_with_acked_copc_fragments,
            TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
        ),
    ],
)
def test_returns_transfer_status_process_failure_with_reason(test_case, expected_reason):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation(
        gp2gp_messages, probe=mock_gp2gp_conversation_observability_probe
    )
    actual = TransferOutcome.from_gp2gp_conversation(conversation, None)

    assert actual.status == TransferStatus.PROCESS_FAILURE
    assert actual.failure_reason == expected_reason


def test_returns_transferred_not_integrated_with_error_given_stalled_with_ehr_and_sender_error():
    conversation = build_mock_gp2gp_conversation()

    conversation.is_integrated.return_value = False
    conversation.has_concluded_with_failure.return_value = False
    conversation.contains_copc_fragments.return_value = False
    conversation.contains_fatal_sender_error_code.return_value = False
    conversation.is_missing_request_acknowledged.return_value = False
    conversation.is_missing_core_ehr.return_value = False
    conversation.contains_core_ehr_with_sender_error.return_value = True

    actual = TransferOutcome.from_gp2gp_conversation(conversation, None)

    assert actual.status == TransferStatus.UNCLASSIFIED_FAILURE
    assert actual.failure_reason == TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR


def test_returns_unclassified_given_unacknowledged_ehr_with_duplicate_and_copc_fragments():
    conversation = build_mock_gp2gp_conversation()

    conversation.is_integrated.return_value = False
    conversation.has_concluded_with_failure.return_value = False
    conversation.contains_unacknowledged_duplicate_ehr_and_copc_fragments.return_value = True

    actual = TransferOutcome.from_gp2gp_conversation(conversation, None)

    assert actual.status == TransferStatus.UNCLASSIFIED_FAILURE
    assert actual.failure_reason == TransferFailureReason.AMBIGUOUS_COPCS


def test_return_process_failure_given_an_unacknowledged_ehr_with_duplicate_and_no_copc_fragments():
    gp2gp_messages: List[Message] = test_cases.acknowledged_duplicate_and_waiting_for_integration()
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = TransferOutcome.from_gp2gp_conversation(conversation, None)

    assert actual.status == TransferStatus.PROCESS_FAILURE
    assert actual.failure_reason == TransferFailureReason.TRANSFERRED_NOT_INTEGRATED


def test_returns_transferred_not_integrated_with_error_given_stalled_with_copc_error():
    conversation = build_mock_gp2gp_conversation()

    conversation.is_integrated.return_value = False
    conversation.has_concluded_with_failure.return_value = False
    conversation.contains_copc_fragments.return_value = True
    conversation.contains_unacknowledged_duplicate_ehr_and_copc_fragments.return_value = False
    conversation.contains_copc_error.return_value = True
    conversation.is_missing_copc_ack.return_value = False

    actual = TransferOutcome.from_gp2gp_conversation(conversation, None)

    assert actual.status == TransferStatus.UNCLASSIFIED_FAILURE
    assert actual.failure_reason == TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR


@pytest.mark.parametrize("fatal_sender_error_code", FATAL_SENDER_ERROR_CODES)
def test_returns_correct_transfer_outcome_if_fatal_sender_error_code_present(
    fatal_sender_error_code,
):
    gp2gp_messages: List[Message] = test_cases.request_acknowledged_with_error(
        error_code=fatal_sender_error_code
    )
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = TransferOutcome.from_gp2gp_conversation(conversation, a_duration())

    assert actual.status == TransferStatus.TECHNICAL_FAILURE
    assert actual.failure_reason == TransferFailureReason.FATAL_SENDER_ERROR


def test_returns_correct_transfer_outcome_given_multiple_conflicting_sender_acks():
    a_fatal_sender_error = choice(FATAL_SENDER_ERROR_CODES)

    gp2gp_messages: List[Message] = test_cases.multiple_sender_acknowledgements(
        error_codes=[None, a_fatal_sender_error]
    )
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = TransferOutcome.from_gp2gp_conversation(conversation, a_duration())

    assert actual.status == TransferStatus.TECHNICAL_FAILURE
    assert actual.failure_reason == TransferFailureReason.FATAL_SENDER_ERROR
