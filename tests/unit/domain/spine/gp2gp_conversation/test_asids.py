from typing import List

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases
from tests.builders.spine import build_gp2gp_conversation, build_message


def test_extracts_sending_practice_asid():
    gp2gp_messages: List[Message] = test_cases.gp2gp_request_made(sending_asid="121212121212")

    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.sending_practice_asid()

    expected = "121212121212"

    assert actual == expected


def test_extracts_requesting_practice_asid():
    conversation = build_gp2gp_conversation(
        request_started=build_message(from_party_asid="123456789012")
    )

    actual = conversation.requesting_practice_asid()

    expected = "123456789012"

    assert actual == expected
