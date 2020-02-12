from datetime import datetime
from typing import NamedTuple, Optional, List

EHR_REQUEST_STARTED = "urn:nhs:names:services:gp2gp/RCMR_IN010000UK05"
EHR_REQUEST_COMPLETED = "urn:nhs:names:services:gp2gp/RCMR_IN030000UK06"
APPLICATION_ACK = "urn:nhs:names:services:gp2gp/MCCI_IN010000UK13"
COMMON_POINT_TO_POINT = "urn:nhs:names:services:gp2gp/COPC_IN000001UK01"


class Message(NamedTuple):
    time: datetime
    conversation_id: str
    guid: str
    interaction_id: str
    from_party_ods_code: str
    to_party_ods_code: str
    message_ref: Optional[str]
    error_code: Optional[int]


class Conversation(NamedTuple):
    id: str
    messages: List[Message]


class ParsedConversation(NamedTuple):
    id: str
    request_started: Message
    request_completed: Message
    request_completed_ack: Optional[Message]
