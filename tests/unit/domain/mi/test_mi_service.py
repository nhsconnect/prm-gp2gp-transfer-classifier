from prmdata.domain.mi.event_type import EventType
from prmdata.domain.mi.mi_message import (
    Attachment,
    Coding,
    Codings,
    Degrade,
    DemographicTraceStatus,
    DocumentMigration,
    Error,
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadEhr,
    MiMessagePayloadIntegration,
    MiMessagePayloadRegistration,
    Placeholder,
    StructuredRecordMigration,
    TransferCompatibilityStatus,
    UnsupportedDataItem,
)
from prmdata.domain.mi.mi_service import MiService
from tests.builders.common import a_datetime, a_string, an_integer
from tests.builders.mi_message import build_mi_message


def test_construct_mi_messages_from_mi_events():
    a_conversation_id = "1111-1111-1111-1111"
    an_event_id = "1234-5678-8765-4321"
    an_event_type = EventType.REGISTRATION_STARTED
    a_protocol = "PRE_TRANSFER"
    a_random_datetime = a_datetime()
    a_supplier = a_string()
    an_ods_code = a_string()
    another_ods_code = a_string()
    another_datetime = a_datetime()
    a_registration_type = "newRegistrant"
    a_status = "MERGED"
    a_reason = "reason"
    ehr_size = an_integer()
    ehr_structured_size = an_integer()
    a_type = "core_ehr"
    some_metadata = "metadata"
    a_code = an_integer()
    some_system = a_string()
    an_attachment_id = an_integer()
    a_clinician_type = a_string()
    a_mime_type = "application"
    some_size_bytes = an_integer()
    a_placeholder_id = a_string()
    another_attachment_id = an_integer()
    some_generated_by = a_string()
    a_reason_code = an_integer()
    another_mime_type = "audio"
    another_placeholder_id = a_string()
    another_another_attachment_id = an_integer()
    another_generated_by = a_string()
    another_reason_code = an_integer()
    another_another_mime_type = "audio"
    an_unsupported_type = a_string()
    an_unsupported_unique_identifier = a_string()
    an_unsupported_reason = a_string()
    an_error_code = an_integer()
    an_error_description = a_string()
    a_transfer_compatibility_status = a_string()
    a_transfer_compatibility_reason = a_string()
    a_smartcard_present = True
    a_demographic_trace_status = "FAILURE"
    a_demographic_trace_reason = a_string()
    a_structured_record_migration_status = "SUCCESS"
    a_structured_record_migration_reason = a_string()
    document_migration_status = "SUCCESS"
    document_migration_reason = a_string()

    mi_events = [
        {
            "conversationId": a_conversation_id,
            "eventId": an_event_id,
            "eventType": an_event_type.value,
            "transferProtocol": a_protocol,
            "eventGeneratedDateTime": a_random_datetime.isoformat(),
            "reportingSystemSupplier": a_supplier,
            "reportingPracticeOdsCode": an_ods_code,
            "transferEventDateTime": another_datetime.isoformat(),
            "payload": {
                "registration": {
                    "registrationType": a_registration_type,
                    "requestingPracticeOdsCode": an_ods_code,
                    "sendingPracticeOdsCode": another_ods_code,
                },
                "integration": {"integrationStatus": a_status, "reason": a_reason},
                "ehr": {
                    "ehrTotalSizeBytes": ehr_size,
                    "ehrStructuredSizeBytes": ehr_structured_size,
                    "degrade": [
                        {
                            "type": a_type,
                            "metadata": some_metadata,
                            "code": {"coding": [{"code": a_code, "system": some_system}]},
                        }
                    ],
                    "attachment": [
                        {
                            "attachmentId": an_attachment_id,
                            "clinicalType": a_clinician_type,
                            "mimeType": a_mime_type,
                            "sizeBytes": some_size_bytes,
                        }
                    ],
                    "placeholder": [
                        {
                            "placeholderId": a_placeholder_id,
                            "attachmentId": another_attachment_id,
                            "generatedBy": some_generated_by,
                            "reason": a_reason_code,
                            "originalMimeType": another_mime_type,
                        },
                        {
                            "placeholderId": another_placeholder_id,
                            "attachmentId": another_another_attachment_id,
                            "generatedBy": another_generated_by,
                            "reason": another_reason_code,
                            "originalMimeType": another_another_mime_type,
                        },
                    ],
                    "unsupportedDataItem": [
                        {
                            "type": an_unsupported_type,
                            "uniqueIdentifier": an_unsupported_unique_identifier,
                            "reason": an_unsupported_reason,
                        }
                    ],
                    "error": [
                        {"errorCode": an_error_code, "errorDescription": an_error_description}
                    ],
                },
                "transferCompatibilityStatus": {
                    "status": a_transfer_compatibility_status,
                    "reason": a_transfer_compatibility_reason,
                },
                "demographicTraceStatus": {
                    "status": a_demographic_trace_status,
                    "reason": a_demographic_trace_reason,
                },
                "smartcardPresent": a_smartcard_present,
                "structuredRecordMigration": {
                    "status": a_structured_record_migration_status,
                    "reason": a_structured_record_migration_reason,
                },
                "documentMigration": {
                    "status": document_migration_status,
                    "reason": document_migration_reason,
                },
            },
        }
    ]

    expected = [
        MiMessage(
            conversation_id=a_conversation_id,
            event_id=an_event_id,
            event_type=an_event_type,
            transfer_protocol=a_protocol,
            event_generated_datetime=a_random_datetime,
            reporting_system_supplier=a_supplier,
            reporting_practice_ods_code=an_ods_code,
            transfer_event_datetime=another_datetime,
            payload=MiMessagePayload(
                registration=MiMessagePayloadRegistration(
                    registration_type=a_registration_type,
                    requesting_practice_ods_code=an_ods_code,
                    sending_practice_ods_code=another_ods_code,
                ),
                integration=MiMessagePayloadIntegration(
                    integration_status=a_status, reason=a_reason
                ),
                ehr=MiMessagePayloadEhr(
                    ehr_total_size_bytes=ehr_size,
                    ehr_structured_size_bytes=ehr_structured_size,
                    degrade=[
                        Degrade(
                            type=a_type,
                            metadata=some_metadata,
                            code=Codings(coding=[Coding(code=a_code, system=some_system)]),
                        )
                    ],
                    attachment=[
                        Attachment(
                            attachment_id=an_attachment_id,
                            clinical_type=a_clinician_type,
                            mime_type=a_mime_type,
                            size_bytes=some_size_bytes,
                        )
                    ],
                    placeholder=[
                        Placeholder(
                            placeholder_id=a_placeholder_id,
                            attachment_id=another_attachment_id,
                            generated_by=some_generated_by,
                            reason=a_reason_code,
                            original_mime_type=another_mime_type,
                        ),
                        Placeholder(
                            placeholder_id=another_placeholder_id,
                            attachment_id=another_another_attachment_id,
                            generated_by=another_generated_by,
                            reason=another_reason_code,
                            original_mime_type=another_another_mime_type,
                        ),
                    ],
                    unsupported_data_item=[
                        UnsupportedDataItem(
                            type=an_unsupported_type,
                            unique_identifier=an_unsupported_unique_identifier,
                            reason=an_unsupported_reason,
                        )
                    ],
                    error=[Error(error_code=an_error_code, error_description=an_error_description)],
                ),
                transfer_compatibility_status=TransferCompatibilityStatus(
                    status=a_transfer_compatibility_status, reason=a_transfer_compatibility_reason
                ),
                demographic_trace_status=DemographicTraceStatus(
                    status=a_demographic_trace_status, reason=a_demographic_trace_reason
                ),
                smartcard_present=a_smartcard_present,
                structured_record_migration=StructuredRecordMigration(
                    status=a_structured_record_migration_status,
                    reason=a_structured_record_migration_reason,
                ),
                document_migration=DocumentMigration(
                    status=document_migration_status, reason=document_migration_reason
                ),
            ),
        )
    ]

    actual = MiService().construct_mi_messages_from_mi_events(mi_events=mi_events)

    assert actual == expected


def test_handles_missing_fields_when_construct_mi_messages_from_mi_events():
    a_conversation_id = "1111-1111-1111-1111"
    an_event_id = "1234-5678-8765-4321"
    an_event_type = EventType.REGISTRATION_STARTED
    a_protocol = "PRE_TRANSFER"
    a_random_datetime = a_datetime()
    a_supplier = a_string()
    an_ods_code = a_string()
    another_datetime = a_datetime()

    mi_events = [
        {
            "conversationId": a_conversation_id,
            "eventId": an_event_id,
            "eventType": an_event_type.value,
            "transferProtocol": a_protocol,
            "eventGeneratedDateTime": a_random_datetime.isoformat(),
            "reportingSystemSupplier": a_supplier,
            "reportingPracticeOdsCode": an_ods_code,
            "transferEventDateTime": another_datetime.isoformat(),
            "payload": {},
        }
    ]

    expected = [
        MiMessage(
            conversation_id=a_conversation_id,
            event_id=an_event_id,
            event_type=an_event_type,
            transfer_protocol=a_protocol,
            event_generated_datetime=a_random_datetime,
            reporting_system_supplier=a_supplier,
            reporting_practice_ods_code=an_ods_code,
            transfer_event_datetime=another_datetime,
            payload=MiMessagePayload(
                registration=MiMessagePayloadRegistration(
                    registration_type=None,
                    requesting_practice_ods_code=None,
                    sending_practice_ods_code=None,
                ),
                integration=MiMessagePayloadIntegration(integration_status=None, reason=None),
                ehr=MiMessagePayloadEhr(
                    ehr_total_size_bytes=None,
                    ehr_structured_size_bytes=None,
                    degrade=[],
                    attachment=[],
                    placeholder=[],
                    unsupported_data_item=[],
                    error=[],
                ),
                transfer_compatibility_status=TransferCompatibilityStatus(status=None, reason=None),
                demographic_trace_status=DemographicTraceStatus(status=None, reason=None),
                smartcard_present=None,
                structured_record_migration=StructuredRecordMigration(status=None, reason=None),
                document_migration=DocumentMigration(status=None, reason=None),
            ),
        )
    ]

    actual = MiService().construct_mi_messages_from_mi_events(mi_events)

    assert actual == expected


def test_group_mi_messages_by_conversation_id():
    conversation_id_one = "1111-1111-1111-1111"
    conversation_id_two = "2222-2222-2222-2222"
    conversation_id_three = "3333-3333-3333-3333"

    mi_message_one = build_mi_message(conversation_id=conversation_id_one)
    mi_message_two = build_mi_message(conversation_id=conversation_id_two)
    mi_message_three = build_mi_message(conversation_id=conversation_id_one)
    mi_message_four = build_mi_message(conversation_id=conversation_id_three)

    mi_events = [mi_message_one, mi_message_two, mi_message_three, mi_message_four]

    expected = {
        conversation_id_one: [mi_message_one, mi_message_three],
        conversation_id_two: [mi_message_two],
        conversation_id_three: [mi_message_four],
    }

    actual = MiService().group_mi_messages_by_conversation_id(mi_events)

    assert actual == expected
