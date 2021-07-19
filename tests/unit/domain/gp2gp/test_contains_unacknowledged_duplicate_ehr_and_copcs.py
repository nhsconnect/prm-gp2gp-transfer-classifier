from typing import List

from tests.builders import test_cases
from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message


def test_returns_true_given_unacknowledged_duplicate_ehr_and_copcs():
    gp2gp_messages: List[
        Message
    ] = test_cases.unacknowledged_duplicate_with_copcs_and_waiting_for_integration()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.contains_unacknowledged_duplicate_ehr_and_copcs()

    assert actual


def test_returns_false_given_acknowledged_duplicate_ehr_and_no_copcs():
    gp2gp_messages: List[Message] = test_cases.acknowledged_duplicate_and_waiting_for_integration()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    actual = conversation.contains_unacknowledged_duplicate_ehr_and_copcs()

    assert actual is False
