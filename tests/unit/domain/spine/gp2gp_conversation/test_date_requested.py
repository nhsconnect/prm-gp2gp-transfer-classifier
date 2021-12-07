from typing import List
from unittest.mock import Mock

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases
from tests.builders.common import a_datetime

mock_gp2gp_conversation_observability_probe = Mock()


def test_extracts_date_requested_from_request_started_message():
    date_requested = a_datetime()

    gp2gp_messages: List[Message] = test_cases.request_made(request_sent_date=date_requested)

    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = conversation.date_requested()

    assert actual == date_requested
