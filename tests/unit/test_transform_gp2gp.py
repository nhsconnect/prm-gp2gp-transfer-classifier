from gp2gp.transformers.gp2gp import build_transfer
from tests.builders.spine import build_parsed_conversation


def test_build_transfer_extracts_conversation_id():
    conversation = build_parsed_conversation(id="1234")

    transfer = build_transfer(conversation)

    expected = "1234"
    actual = transfer.conversation_id
    assert actual == expected
