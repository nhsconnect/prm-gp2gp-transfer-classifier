from typing import List
from unittest.mock import Mock

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases

mock_gp2gp_conversation_observability_probe = Mock()


def test_returns_true_given_unacknowledged_duplicate_ehr_and_copcs():
    gp2gp_messages: List[
        Message
    ] = test_cases.unacknowledged_duplicate_with_copcs_and_waiting_for_integration()
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = conversation.contains_unacknowledged_duplicate_ehr_and_copc_fragments()

    assert actual


def test_returns_false_given_acknowledged_duplicate_ehr_and_no_copcs():
    gp2gp_messages: List[Message] = test_cases.acknowledged_duplicate_and_waiting_for_integration()
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = conversation.contains_unacknowledged_duplicate_ehr_and_copc_fragments()

    assert actual is False


def test_returns_true_given_only_duplicate_ehrs_present():
    gp2gp_messages: List[Message] = test_cases.only_acknowledged_duplicates()
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = conversation.contains_only_duplicate_ehr()

    assert actual


def test_returns_false_given_an_unacknowledged_ehr_present():
    gp2gp_messages: List[Message] = test_cases.acknowledged_duplicate_and_waiting_for_integration()
    conversation = Gp2gpConversation(gp2gp_messages, mock_gp2gp_conversation_observability_probe)

    actual = conversation.contains_only_duplicate_ehr()

    assert actual is False
