from typing import List

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases
from tests.builders.common import a_datetime


def test_extracts_date_requested_from_request_started_message():
    date_requested = a_datetime()

    gp2gp_messages: List[Message] = test_cases.gp2gp_request_made(request_sent_date=date_requested)

    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.date_requested()

    assert actual == date_requested
