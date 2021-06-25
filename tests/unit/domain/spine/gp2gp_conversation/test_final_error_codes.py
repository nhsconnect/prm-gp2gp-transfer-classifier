from typing import List

from tests.builders.spine import build_gp2gp_conversation, build_message


def test_extracts_final_error_codes():
    conversation = build_gp2gp_conversation(
        request_completed_ack_messages=[
            build_message(error_code=99),
            build_message(error_code=1),
            build_message(error_code=None),
        ]
    )

    actual = conversation.final_error_codes()

    expected = [99, 1, None]

    assert actual == expected


def test_doesnt_extract_error_code_given_pending_request_completed_ack():
    conversation = build_gp2gp_conversation(request_completed_ack_messages=[])

    actual = conversation.final_error_codes()

    expected: List[int] = []

    assert actual == expected
