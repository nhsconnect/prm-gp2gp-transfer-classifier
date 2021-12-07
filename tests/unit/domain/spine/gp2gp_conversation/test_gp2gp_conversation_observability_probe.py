from unittest.mock import Mock

from prmdata.domain.spine.gp2gp_conversation import (
    Gp2gpConversation,
    Gp2gpConversationObservabilityProbe,
)
from prmdata.domain.spine.message import EHR_REQUEST_STARTED
from tests.builders.spine import build_message
from tests.builders.test_cases import ehr_missing_message_for_an_acknowledgement


def test_warning_when_missing_message_for_an_acknowledgement():
    mock_logger = Mock()
    mock_probe = Gp2gpConversationObservabilityProbe(mock_logger)

    messages = ehr_missing_message_for_an_acknowledgement()
    acknowledgement_for_missing_message = messages[1]

    Gp2gpConversation(messages=messages, probe=mock_probe)

    message_ref = acknowledgement_for_missing_message.message_ref

    mock_logger.warning.assert_called_once_with(
        f":Couldn't pair acknowledgement with message for ref: {message_ref}",
        extra={
            "event": "MISSING_MESSAGE_FOR_ACKNOWLEDGEMENT",
            "conversation_id": acknowledgement_for_missing_message.conversation_id,
        },
    )


def test_warning_when_unable_to_determine_purpose_of_message():
    mock_logger = Mock()
    mock_probe = Gp2gpConversationObservabilityProbe(mock_logger)

    unknown_message_purpose_message = build_message(
        conversation_id="ASD",
        guid="abc",
        interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN010000UK08",
    )
    messages = [
        build_message(
            conversation_id="ASD",
            interaction_id=EHR_REQUEST_STARTED,
        ),
        unknown_message_purpose_message,
    ]

    Gp2gpConversation(messages=messages, probe=mock_probe)

    mock_logger.warning.assert_called_once_with(
        f":Couldn't determine purpose of message with guid: {unknown_message_purpose_message.guid}",
        extra={
            "event": "UNKNOWN_MESSAGE_PURPOSE",
            "conversation_id": unknown_message_purpose_message.conversation_id,
            "interaction_id": unknown_message_purpose_message.interaction_id,
        },
    )
