from typing import List

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_made,
        test_cases.request_acknowledged_successfully,
        test_cases.core_ehr_sent,
        test_cases.large_message_continue_sent,
        test_cases.pending_integration_with_large_message_fragments,
        test_cases.pending_integration_with_acked_large_message_fragments,
    ],
)
def test_doesnt_extract_error_codes_given_transfer_in_progress(test_case):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.final_error_codes()

    expected: List[int] = []

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.large_message_fragment_failure,
        test_cases.multiple_large_fragment_failures,
    ],
)
def test_doesnt_extract_large_message_error_codes(test_case):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.final_error_codes()

    expected: List[int] = []

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_acknowledged_with_error,
        test_cases.core_ehr_sent_with_sender_error,
    ],
)
def test_doesnt_extract_sender_error_codes(test_case):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected: List[int] = []

    actual = conversation.final_error_codes()

    assert actual == expected


def test_extracts_correct_code_given_waiting_for_integration_with_duplicate():
    gp2gp_messages = test_cases.acknowledged_duplicate_and_waiting_for_integration()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = [12]

    actual = conversation.final_error_codes()

    assert actual == expected


@pytest.mark.parametrize(
    ["test_case", "expected_codes"],
    [
        (test_cases.ehr_integrated_successfully, [None]),
        (test_cases.ehr_suppressed, [15]),
        (test_cases.ehr_integrated_after_duplicate, [12, None]),
    ],
)
def test_extracts_correct_codes_given_successful_transfer(test_case, expected_codes):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.final_error_codes()

    assert actual == expected_codes


def test_extracts_correct_codes_given_transfer_concluded_with_failure():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.ehr_integration_failed(error_code=42)
    )

    expected = [42]

    actual = conversation.final_error_codes()

    assert actual == expected


def test_extracts_correct_codes_given_duplicate_and_failure():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.integration_failed_after_duplicate(error_code=42)
    )

    expected = [12, 42]

    actual = conversation.final_error_codes()

    assert actual == expected


def test_extracts_corrects_codes_given_success_and_failure():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.second_ehr_integrated_before_first_ehr_failed(error_code=42)
    )

    expected = [None, 42]

    actual = conversation.final_error_codes()

    assert actual == expected


def test_extracts_correct_codes_given_multiple_failures():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.multiple_integration_failures(error_codes=[42, 99, 56])
    )

    expected = [42, 99, 56]

    actual = conversation.final_error_codes()

    assert actual == expected
