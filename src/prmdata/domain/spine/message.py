from datetime import datetime
from typing import Iterable, List, NamedTuple, Optional

from dateutil import parser

EHR_REQUEST_STARTED = "urn:nhs:names:services:gp2gp/RCMR_IN010000UK05"
EHR_REQUEST_COMPLETED = "urn:nhs:names:services:gp2gp/RCMR_IN030000UK06"
APPLICATION_ACK = "urn:nhs:names:services:gp2gp/MCCI_IN010000UK13"
COMMON_POINT_TO_POINT = "urn:nhs:names:services:gp2gp/COPC_IN000001UK01"

ERROR_SUPPRESSED = 15
DUPLICATE_ERROR = 12
FATAL_SENDER_ERROR_CODES = [6, 7, 10, 14, 23, 24, 99, 30]


class Message(NamedTuple):
    time: datetime
    conversation_id: str
    guid: str
    interaction_id: str
    from_party_asid: str
    to_party_asid: str
    message_ref: Optional[str]
    error_code: Optional[int]
    from_system: Optional[str]
    to_system: Optional[str]

    def is_ehr_request_started(self):
        return self.interaction_id == EHR_REQUEST_STARTED

    def is_ehr_request_completed(self):
        return self.interaction_id == EHR_REQUEST_COMPLETED

    def is_acknowledgement_of(self, other_message):
        return self.interaction_id == APPLICATION_ACK and self.message_ref == other_message.guid

    def is_acknowledgement(self):
        return self.interaction_id == APPLICATION_ACK

    def is_copc(self):
        return self.interaction_id == COMMON_POINT_TO_POINT


def _parse_error_code(error):
    return None if error == "NONE" else int(error)


def _parse_message_ref(ref):
    return None if ref == "NotProvided" else ref


def construct_messages_from_splunk_items(items: Iterable[dict]) -> List[Message]:
    return [
        Message(
            time=parser.parse(item["_time"]),
            conversation_id=item["conversationID"],
            guid=item["GUID"],
            interaction_id=item["interactionID"],
            from_party_asid=item["messageSender"],
            to_party_asid=item["messageRecipient"],
            message_ref=_parse_message_ref(item["messageRef"]),
            error_code=_parse_error_code(item["jdiEvent"]),
            from_system=item.get("fromSystem"),
            to_system=item.get("toSystem"),
        )
        for item in items
    ]
