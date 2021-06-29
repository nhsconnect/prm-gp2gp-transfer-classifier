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
    ],
)
def test_doesnt_extract_error_code_given_transfer_in_progress(test_case):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.final_error_codes()

    expected: List[int] = []

    assert actual == expected


@pytest.mark.parametrize(
    ["test_case", "expected_codes"],
    [
        (test_cases.ehr_integrated_successfully, [None]),
        (test_cases.suppressed_ehr, [15]),
        (test_cases.ehr_integrated_after_duplicate, [12, None]),
    ],
)
def test_extracts_correct_code_given_successful_transfer(test_case, expected_codes):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.final_error_codes()

    assert actual == expected_codes


def test_extracts_correct_code_given_transfer_concluded_with_failure():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.concluded_with_failure(error_code=42)
    )

    expected = [42]

    actual = conversation.final_error_codes()

    assert actual == expected


def test_extracts_correct_code_given_duplicate_and_failure():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.integration_failed_after_duplicate(error_code=42)
    )

    expected = [12, 42]

    actual = conversation.final_error_codes()

    assert actual == expected


def test_extracts_correct_code_given_success_and_failure():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.second_ehr_integrated_before_first_ehr_failed(error_code=42)
    )

    expected = [None, 42]

    actual = conversation.final_error_codes()

    assert actual == expected


def test_correct_code_given_multiple_failures():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.multiple_integration_failures(error_codes=[42, 99, 56])
    )

    expected = [42, 99, 56]

    actual = conversation.final_error_codes()

    assert actual == expected
