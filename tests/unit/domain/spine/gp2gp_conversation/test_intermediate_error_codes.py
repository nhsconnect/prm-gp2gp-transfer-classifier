from typing import List

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases


def test_extracts_an_intermediate_message_error_code():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.large_message_fragment_failure(error_code=20)
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [20]

    assert actual == expected_intermediate_error_codes


def test_intermediate_error_code_is_empty_list_if_no_errors():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.successful_integration_with_large_messages()
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes: List[int] = []

    assert actual == expected_intermediate_error_codes


def test_extracts_multiple_intermediate_message_error_codes():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.multiple_large_fragment_failures(error_codes=[11, None, 10])
    )

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [11, 10]

    assert actual == expected_intermediate_error_codes
