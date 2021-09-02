from unittest.mock import Mock

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases

mock_gp2gp_conversation_observability_probe = Mock()


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_made,
        test_cases.request_acknowledged_successfully,
        test_cases.core_ehr_sent,
        test_cases.core_ehr_sent_with_sender_error,
        test_cases.copc_continue_sent,
        test_cases.acknowledged_duplicate_and_waiting_for_integration,
        test_cases.pending_integration_with_acked_copc_fragments,
    ],
)
def test_returns_false_when_transfer_is_still_in_progress(test_case):
    conversation = Gp2gpConversation(
        messages=test_case(), probe=mock_gp2gp_conversation_observability_probe
    )

    expected = False

    actual = conversation.is_integrated()

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.ehr_integration_failed,
        test_cases.integration_failed_after_duplicate,
        test_cases.multiple_integration_failures,
        test_cases.copc_fragment_failure,
        test_cases.request_acknowledged_with_error,
        test_cases.copc_fragment_failures,
        test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs,
    ],
)
def test_returns_false_when_transfer_concluded_with_failure(test_case):
    conversation = Gp2gpConversation(
        messages=test_case(), probe=mock_gp2gp_conversation_observability_probe
    )

    expected = False

    actual = conversation.is_integrated()

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.ehr_integrated_successfully,
        test_cases.ehr_integrated_late,
        test_cases.ehr_suppressed,
        test_cases.ehr_integrated_after_duplicate,
        test_cases.first_ehr_integrated_after_second_ehr_failed,
        test_cases.first_ehr_integrated_before_second_ehr_failed,
        test_cases.second_ehr_integrated_after_first_ehr_failed,
        test_cases.second_ehr_integrated_before_first_ehr_failed,
        test_cases.successful_integration_with_copc_fragments,
        test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack,
        test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack,
    ],
)
def test_returns_true_when_transfer_was_integrated(test_case):
    conversation = Gp2gpConversation(
        messages=test_case(), probe=mock_gp2gp_conversation_observability_probe
    )

    expected = True

    actual = conversation.is_integrated()

    assert actual == expected
