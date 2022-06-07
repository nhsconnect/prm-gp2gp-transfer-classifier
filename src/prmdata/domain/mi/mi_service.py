from dataclasses import dataclass
from typing import List


@dataclass
class MiMessage:
    conversation_id: str
    event_id: str
    event_generated_datetime: str
    reporting_system_supplier: str
    reporting_practice_ods_code: str
    transfer_event_datetime: str


class MiService:
    @staticmethod
    def construct_mi_messages_from_mi_events(mi_events: List[dict]) -> List[MiMessage]:
        return [
            MiMessage(
                conversation_id=event["conversationId"],
                event_id=event["eventId"],
                event_generated_datetime=event["eventGeneratedDateTime"],
                reporting_system_supplier=event["reportingSystemSupplier"],
                reporting_practice_ods_code=event["reportingPracticeOdsCode"],
                transfer_event_datetime=event["transferEventDateTime"],
            )
            for event in mi_events
        ]
