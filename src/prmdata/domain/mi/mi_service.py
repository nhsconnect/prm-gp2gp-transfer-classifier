from datetime import datetime
from typing import List, Optional

from prmdata.domain.mi.mi_message import (
    Attachment,
    Coding,
    Codings,
    Degrade,
    DemographicTraceStatus,
    Error,
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadEhr,
    MiMessagePayloadIntegration,
    MiMessagePayloadRegistration,
    Placeholder,
    TransferCompatibilityStatus,
    UnsupportedDataItem,
)
from prmdata.domain.mi.mi_transfer import EventSummary, MiPractice, MiTransfer

GroupedMiMessages = dict[str, List[MiMessage]]


class MiService:
    def __init__(self):
        pass

    @staticmethod
    def _get_payload_ehr(event: dict) -> dict:
        return event.get("payload", {}).get("ehr", {})

    @staticmethod
    def _create_unsupported_data_item(
        unsupported_data_item_list: List[dict],
    ) -> Optional[List[UnsupportedDataItem]]:

        return [
            UnsupportedDataItem(
                type=unsupported_data_item.get("type"),
                unique_identifier=unsupported_data_item.get("uniqueIdentifier"),
                reason=unsupported_data_item.get("reason"),
            )
            for unsupported_data_item in unsupported_data_item_list
        ]

    @staticmethod
    def _create_error(
        error_list: List[dict],
    ) -> Optional[List[Error]]:
        return [
            Error(
                error_code=error.get("errorCode"),
                error_description=error.get("errorDescription"),
            )
            for error in error_list
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
                event_generated_datetime=datetime.strptime(
                    event["eventGeneratedDateTime"], "%Y-%m-%dT%H:%M:%S%z"
                ),
                reporting_system_supplier=event["reportingSystemSupplier"],
                reporting_practice_ods_code=event["reportingPracticeOdsCode"],
                transfer_event_datetime=datetime.strptime(
                    event["transferEventDateTime"], "%Y-%m-%dT%H:%M:%S%z"
                ),
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
                        error=self._create_error(self._get_payload_ehr(event).get("error", [])),
                    ),
                    transfer_compatibility_status=TransferCompatibilityStatus(
                        status=event.get("payload", {})
                        .get("transferCompatibilityStatus", {})
                        .get("status"),
                        reason=event.get("payload", {})
                        .get("transferCompatibilityStatus", {})
                        .get("reason"),
                    ),
                    demographic_trace_status=DemographicTraceStatus(
                        status=event.get("payload", {})
                        .get("demographicTraceStatus", {})
                        .get("status"),
                        reason=event.get("payload", {})
                        .get("demographicTraceStatus", {})
                        .get("reason"),
                    ),
                    smartcard_present=event.get("payload", {}).get("smartcardPresent"),
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

    def convert_to_mi_transfers(self, grouped_messages: GroupedMiMessages) -> List[MiTransfer]:
        mi_transfers: List[MiTransfer] = []

        for messages in grouped_messages.values():
            requesting_supplier = messages[0].reporting_system_supplier

            sending_supplier = None
            if len(messages) > 1:
                sending_supplier = messages[1].reporting_system_supplier

            mi_transfers.append(
                MiTransfer(
                    conversation_id=messages[0].conversation_id,
                    events=[
                        EventSummary(
                            event_generated_datetime=message.event_generated_datetime,
                            event_type=message.event_type,
                            event_id=message.event_id,
                        )
                        for message in messages
                    ],
                    requesting_practice=MiPractice(
                        supplier=requesting_supplier,
                        ods_code=messages[0].payload.registration.requesting_practice_ods_code,
                    ),
                    sending_practice=MiPractice(
                        sending_supplier,
                        ods_code=messages[0].payload.registration.sending_practice_ods_code,
                    ),
                )
            )
        return mi_transfers
