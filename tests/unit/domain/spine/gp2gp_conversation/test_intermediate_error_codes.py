from typing import List
from unittest.mock import Mock

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases

mock_gp2gp_conversation_observability_probe = Mock()


def test_extracts_an_intermediate_message_error_code():
    conversation = Gp2gpConversation(
        messages=test_cases.copc_fragment_failure(error_code=20),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [20]

    assert actual == expected_intermediate_error_codes


def test_intermediate_error_code_is_empty_list_if_no_errors():
    conversation = Gp2gpConversation(
        messages=test_cases.successful_integration_with_copc_fragments(),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes: List[int] = []

    assert actual == expected_intermediate_error_codes


def test_extracts_error_codes_when_some_messages_unacknowledged():
    conversation = Gp2gpConversation(
        messages=test_cases.copc_fragment_failure_and_missing_copc_fragment_ack(error_code=10),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [10]

    assert actual == expected_intermediate_error_codes


def test_extracts_multiple_intermediate_message_error_codes():
    conversation = Gp2gpConversation(
        messages=test_cases.copc_fragment_failures(error_codes=[11, None, 10]),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [11, 10]

    assert actual == expected_intermediate_error_codes


def test_ignores_sender_error_codes():
    conversation = Gp2gpConversation(
        messages=test_cases.request_acknowledged_with_error(),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes: List[int] = []

    assert actual == expected_intermediate_error_codes


def test_ignores_ehr_acknowledgement_error_codes():
    conversation = Gp2gpConversation(
        messages=test_cases.ehr_integration_failed(),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes: List[int] = []

    assert actual == expected_intermediate_error_codes


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_made,
        test_cases.request_acknowledged_successfully,
        test_cases.core_ehr_sent,
        test_cases.copc_continue_sent,
        test_cases.acknowledged_duplicate_and_waiting_for_integration,
        test_cases.pending_integration_with_acked_copc_fragments,
    ],
)
def test_returns_nothing_when_transfer_in_progress_and_no_errors(test_case):
    conversation = Gp2gpConversation(
        messages=test_cases.ehr_integration_failed(),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes: List[int] = []

    assert actual == expected_intermediate_error_codes
