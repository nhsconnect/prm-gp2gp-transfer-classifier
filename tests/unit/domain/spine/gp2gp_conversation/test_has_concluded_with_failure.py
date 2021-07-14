import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_made,
        test_cases.request_acknowledged_successfully,
        test_cases.large_message_continue_sent,
        test_cases.core_ehr_sent,
        test_cases.core_ehr_sent_with_sender_error,
        test_cases.acknowledged_duplicate_and_waiting_for_integration,
        test_cases.pending_integration_with_acked_large_message_fragments,
        test_cases.large_message_fragment_failure_and_missing_large_fragment_ack,
    ],
)
def test_returns_false_given_pending_transfer(test_case):
    conversation = Gp2gpConversation.from_messages(messages=test_case())

    expected = False

    actual = conversation.has_concluded_with_failure()

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
        test_cases.successful_integration_with_large_messages,
        test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack,
        test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack,
    ],
)
def test_returns_false_given_successful_transfer(test_case):
    conversation = Gp2gpConversation.from_messages(messages=test_case())

    expected = False

    actual = conversation.has_concluded_with_failure()

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_acknowledged_with_error,
        test_cases.large_message_fragment_failure,
        test_cases.multiple_large_fragment_failures,
    ],
)
def test_returns_false_given_intermediate_error(test_case):
    conversation = Gp2gpConversation.from_messages(messages=test_case())

    expected = False

    actual = conversation.has_concluded_with_failure()

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.ehr_integration_failed,
        test_cases.integration_failed_after_duplicate,
        test_cases.multiple_integration_failures,
        test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs,
    ],
)
def test_returns_true_given_failed_transfer(test_case):
    conversation = Gp2gpConversation.from_messages(messages=test_case())

    expected = True

    actual = conversation.has_concluded_with_failure()

    assert actual == expected
