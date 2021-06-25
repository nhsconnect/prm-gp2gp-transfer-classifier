from typing import List

from tests.builders.spine import build_gp2gp_conversation, build_message


def test_extracts_an_intermediate_message_error_code():
    intermediate_messages = [build_message(error_code=20)]
    conversation = build_gp2gp_conversation(intermediate_messages=intermediate_messages)

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [20]

    assert actual == expected_intermediate_error_codes


def test_intermediate_error_code_is_empty_list_if_no_errors():
    intermediate_messages = [build_message(), build_message(), build_message()]
    conversation = build_gp2gp_conversation(intermediate_messages=intermediate_messages)

    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes: List[int] = []

    assert actual == expected_intermediate_error_codes


def test_extracts_multiple_intermediate_message_error_codes():
    intermediate_messages = [
        build_message(error_code=11),
        build_message(),
        build_message(error_code=10),
    ]
    conversation = build_gp2gp_conversation(intermediate_messages=intermediate_messages)
    actual = conversation.intermediate_error_codes()

    expected_intermediate_error_codes = [11, 10]

    assert actual == expected_intermediate_error_codes
