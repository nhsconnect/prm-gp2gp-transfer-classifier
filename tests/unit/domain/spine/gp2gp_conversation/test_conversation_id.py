from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases


def test_extracts_conversation_id():
    conversation = Gp2gpConversation.from_messages(
        messages=test_cases.request_made(conversation_id="1234")
    )

    expected = "1234"

    actual = conversation.conversation_id()

    assert actual == expected
