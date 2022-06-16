from typing import List, Optional

from prmdata.domain.mi.mi_message import (
    Attachment,
    Coding,
    Codings,
    Degrade,
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadEhr,
    MiMessagePayloadIntegration,
    MiMessagePayloadRegistration,
    Placeholder,
    UnsupportedDataItem,
)

GroupedMiMessages = dict[str, List[MiMessage]]


class MiService:
    def __init__(self):
        pass

    @staticmethod
    def _get_payload_ehr(event: dict) -> dict:
        return event.get("payload", {}).get("ehr", {})

    @staticmethod
    def _create_unsupported_data_item(
        unsupported_data_item: List[dict],
    ) -> Optional[List[UnsupportedDataItem]]:

        return [
            UnsupportedDataItem(
                type=placeholder.get("type"),
                unique_identifier=placeholder.get("uniqueIdentifier"),
                reason=placeholder.get("reason"),
            )
            for placeholder in unsupported_data_item
        ]

    @staticmethod
    def _create_attachment(
        attachment_list: List[dict],
    ) -> Optional[List[Attachment]]:

        return [
            Attachment(
                attachment_id=attachment.get("attachmentId"),
                clinical_type=attachment.get("clinicalType"),
                mime_type=attachment.get("mimeType"),
                size_bytes=attachment.get("sizeBytes"),
            )
            for attachment in attachment_list
        ]

    @staticmethod
    def _create_placeholder(
        placeholder_list: List[dict],
    ) -> Optional[List[Placeholder]]:

        return [
            Placeholder(
                placeholder_id=placeholder.get("placeholderId"),
                attachment_id=placeholder.get("attachmentId"),
                generated_by=placeholder.get("generatedBy"),
                reason=placeholder.get("reason"),
                original_mime_type=placeholder.get("originalMimeType"),
            )
            for placeholder in placeholder_list
        ]

    @staticmethod
    def _create_coding(degrade: dict) -> List[Coding]:
        return [
            Coding(code=coding.get("code"), system=coding.get("system"))
            for coding in degrade.get("code", {}).get("coding")
        ]

    def _create_degrade(self, degrade_list: List[dict]) -> Optional[List[Degrade]]:
        return [
            Degrade(
                type=degrade.get("type"),
                metadata=degrade.get("metadata"),
                code=Codings(coding=self._create_coding(degrade)),
            )
            for degrade in degrade_list
        ]

    def construct_mi_messages_from_mi_events(self, mi_events: List[dict]) -> List[MiMessage]:
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
                        registration_type=event.get("payload", {})
                        .get("registration", {})
                        .get("registration_type"),
                        requesting_practice_ods_code=event.get("payload", {})
                        .get("registration", {})
                        .get("requesting_practice_ods_code"),
                        sending_practice_ods_code=event.get("payload", {})
                        .get("registration", {})
                        .get("sending_practice_ods_code"),
                    ),
                    integration=MiMessagePayloadIntegration(
                        integration_status=event.get("payload", {})
                        .get("integration", {})
                        .get("integration_status"),
                        reason=event.get("payload", {}).get("integration", {}).get("reason"),
                    ),
                    ehr=MiMessagePayloadEhr(
                        ehr_total_size_bytes=self._get_payload_ehr(event).get("ehrTotalSizeBytes"),
                        ehr_structured_size_bytes=self._get_payload_ehr(event).get(
                            "ehrStructuredSizeBytes"
                        ),
                        degrade=self._create_degrade(
                            self._get_payload_ehr(event).get("degrade", [])
                        ),
                        attachment=self._create_attachment(
                            self._get_payload_ehr(event).get("attachment", [])
                        ),
                        placeholder=self._create_placeholder(
                            self._get_payload_ehr(event).get("placeholder", [])
                        ),
                        unsupported_data_item=self._create_unsupported_data_item(
                            self._get_payload_ehr(event).get("unsupportedDataItem", [])
                        ),
                    ),
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
