from dataclasses import dataclass
from typing import List, Optional


@dataclass
class MiMessagePayloadRegistration:
    registrationStartedDateTime: Optional[str]
    registrationType: Optional[str]
    requestingPracticeOdsCode: Optional[str]
    sendingPracticeOdsCode: Optional[str]


@dataclass
class MiMessagePayload:
    registration: Optional[MiMessagePayloadRegistration]


@dataclass
class MiMessage:
    conversation_id: str
    event_id: str
    event_type: str
    transfer_protocol: str
    event_generated_datetime: str
    reporting_system_supplier: str
    reporting_practice_ods_code: str
    transfer_event_datetime: str
    payload: MiMessagePayload


GroupedMiMessages = dict[str, List[MiMessage]]


class MiService:
    @staticmethod
    def construct_mi_messages_from_mi_events(mi_events: List[dict]) -> List[MiMessage]:
        return [
            MiMessage(
                conversation_id=event["conversationId"],
                event_id=event["eventId"],
                event_type=event["eventType"],
                transfer_protocol=event["transferProtocol"],
                event_generated_datetime=event["eventGeneratedDateTime"],
                reporting_system_supplier=event["reportingSystemSupplier"],
                reporting_practice_ods_code=event["reportingPracticeOdsCode"],
                transfer_event_datetime=event["transferEventDateTime"],
                payload=MiMessagePayload(
                    registration=MiMessagePayloadRegistration(
                        registrationStartedDateTime=event.get("payload", {})
                        .get("registration", {})
                        .get("registrationStartedDateTime"),
                        registrationType=event.get("payload", {})
                        .get("registration", {})
                        .get("registrationType"),
                        requestingPracticeOdsCode=event.get("payload", {})
                        .get("registration", {})
                        .get("requestingPracticeOdsCode"),
                        sendingPracticeOdsCode=event.get("payload", {})
                        .get("registration", {})
                        .get("sendingPracticeOdsCode"),
                    )
                ),
            )
            for event in mi_events
        ]

    @staticmethod
    def group_mi_messages_by_conversation_id(
        mi_messages: List[MiMessage],
    ) -> GroupedMiMessages:
        grouped_mi_messages: GroupedMiMessages = {}
        for mi_message in mi_messages:
            if mi_message.conversation_id not in grouped_mi_messages:
                grouped_mi_messages[mi_message.conversation_id] = []

            grouped_mi_messages[mi_message.conversation_id].append(mi_message)

        return grouped_mi_messages
