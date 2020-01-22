from collections import defaultdict
from typing import Iterator, Dict, List, Optional

from gp2gp.models.spine import Conversation, ParsedConversation, Message


EHR_REQUEST_STARTED = "urn:nhs:names:services:gp2gp/RCMR_IN010000UK05"
EHR_REQUEST_COMPLETED = "urn:nhs:names:services:gp2gp/RCMR_IN030000UK06"
APP_ACKNOWLEDGEMENT = "urn:nhs:names:services:gp2gp/MCCI_IN010000UK13"


def group_into_conversations(messages: Iterator[Message]) -> Iterator[Conversation]:
    conversations: Dict[str, List[Message]] = defaultdict(list)

    for message in messages:
        conversations[message.conversation_id].append(message)

    return (
        Conversation(conversation_id, sorted(messages, key=lambda m: m.time))
        for conversation_id, messages in conversations.items()
    )


def parse_conversation(conversation: Conversation) -> Optional[ParsedConversation]:
    parser = SpineConversationParser(conversation)
    return parser.parse()


class SpineConversationParser:
    def __init__(self, conversation: Conversation):
        self._id = conversation.id
        self._messages = iter(conversation.messages)

    def _advance_until_interaction(self, interaction_id):
        return self._advance_until(lambda m: m.interaction_id == interaction_id)

    def _advance_until_acknowledgment_of(self, message):
        return self._advance_until(
            lambda m: m.interaction_id == APP_ACKNOWLEDGEMENT and m.message_ref == message.guid
        )

    def _advance_until(self, func):
        next_message = next(self._messages)
        while not func(next_message):
            next_message = next(self._messages)
        return next_message

    def parse(self):
        req_started_message = next(self._messages)
        if req_started_message.interaction_id != EHR_REQUEST_STARTED:
            return None
        req_completed_message = self._advance_until_interaction(EHR_REQUEST_COMPLETED)
        final_ack = self._advance_until_acknowledgment_of(req_completed_message)
        return ParsedConversation(
            self._id,
            request_started=req_started_message,
            request_completed=req_completed_message,
            request_completed_ack=final_ack,
        )
