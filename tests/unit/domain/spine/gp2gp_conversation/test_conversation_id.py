from unittest.mock import Mock

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases

mock_gp2gp_conversation_observability_probe = Mock()


def test_extracts_conversation_id():
    conversation = Gp2gpConversation(
        messages=test_cases.request_made(conversation_id="1234"),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    expected = "1234"

    actual = conversation.conversation_id()

    assert actual == expected
