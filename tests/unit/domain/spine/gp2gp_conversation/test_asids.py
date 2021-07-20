from typing import List

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases


def test_extracts_sending_practice_asid():
    gp2gp_messages: List[Message] = test_cases.request_made(sending_asid="121212121212")

    conversation = Gp2gpConversation(gp2gp_messages)

    actual = conversation.sending_practice_asid()

    expected = "121212121212"

    assert actual == expected


def test_extracts_requesting_practice_asid():
    gp2gp_messages: List[Message] = test_cases.request_made(requesting_asid="123456789012")

    conversation = Gp2gpConversation(gp2gp_messages)

    actual = conversation.requesting_practice_asid()

    expected = "123456789012"

    assert actual == expected
