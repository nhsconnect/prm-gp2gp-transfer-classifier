from unittest.mock import MagicMock, Mock

from prmdata.domain.mi.event_type import EventType
from prmdata.domain.mi.mi_message import (
    DemographicTraceStatus,
    DocumentMigration,
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadEhr,
    MiMessagePayloadIntegration,
    MiMessagePayloadRegistration,
    StructuredRecordMigration,
)
from prmdata.domain.mi.mi_service import MiService
from prmdata.domain.mi.mi_transfer import MiTransfer
from prmdata.pipeline.io import TransferClassifierIO
from prmdata.pipeline.mi_runner import MiRunner
from prmdata.pipeline.transfer_classifier import RunnerObservabilityProbe
from tests.builders.common import a_datetime
from tests.builders.config import build_config


def test_transfer_classifier_mi_runner_abstract_class():
    an_event_id = "1234"
    a_conversation_id = "1111-1111-1111-1111"
    an_event_type = EventType.EHR_GENERATED
    a_transfer_protocol = "GP2GP"
    a_random_datetime = a_datetime()
    an_integration_status = "merged"
    an_integration_reason = "some reason"
    a_reporting_system_supplier = "ABC"
    a_reporting_practice_ods_code = "ABC123"
    a_demographic_status = "a status"
    a_demographic_reason = "some reason"
    a_structured_record_migration_status = "a status"
    a_structured_record_migration_reason = "a reason"
    a_document_migration_status = "a status"
    a_document_migration_reason = "a reason"

    an_event = {
        "eventId": an_event_id,
        "conversationId": a_conversation_id,
        "eventType": an_event_type.value,
        "transferProtocol": a_transfer_protocol,
        "eventGeneratedDateTime": a_random_datetime,
        "reportingSystemSupplier": a_reporting_system_supplier,
        "reportingPracticeOdsCode": a_reporting_practice_ods_code,
        "transferEventDateTime": a_random_datetime,
        "payload": {
            "integration": {
                "integrationStatus": an_integration_status,
                "reason": an_integration_reason,
            },
            "demographicTraceStatus": {
                "status": a_demographic_status,
                "reason": a_demographic_reason,
            },
            "structuredRecordMigration": {
                "status": a_structured_record_migration_status,
                "reason": a_structured_record_migration_reason,
            },
            "documentMigration": {
                "status": a_document_migration_status,
                "reason": a_document_migration_reason,
            },
        },
    }

    an_mi_message = MiMessage(
        conversation_id=a_conversation_id,
        event_id=an_event_id,
        event_type=an_event_type,
        transfer_protocol=a_transfer_protocol,
        event_generated_datetime=a_random_datetime,
        reporting_system_supplier=a_reporting_system_supplier,
        reporting_practice_ods_code=a_reporting_practice_ods_code,
        transfer_event_datetime=a_random_datetime,
        payload=MiMessagePayload(
            registration=MiMessagePayloadRegistration(
                registration_type=None,
                requesting_practice_ods_code=None,
                sending_practice_ods_code=None,
            ),
            integration=MiMessagePayloadIntegration(
                integration_status=an_integration_status, reason=an_integration_reason
            ),
            ehr=MiMessagePayloadEhr(
                ehr_total_size_bytes=None,
                ehr_structured_size_bytes=None,
                degrade=[],
                attachment=[],
                placeholder=[],
                unsupported_data_item=[],
                error=[],
            ),
            transfer_compatibility_status=None,
            demographic_trace_status=DemographicTraceStatus(
                status=a_demographic_status, reason=a_demographic_reason
            ),
            smartcard_present=None,
            structured_record_migration=StructuredRecordMigration(
                status=a_structured_record_migration_status,
                reason=a_structured_record_migration_reason,
            ),
            document_migration=DocumentMigration(
                status=a_document_migration_status,
                reason=a_document_migration_reason,
            ),
        ),
    )

    mi_messages = [an_mi_message]

    grouped_mi_messages = {a_conversation_id: [an_mi_message]}

    transfers = [
        MiTransfer(
            conversation_id=a_conversation_id,
            events=[],
            requesting_practice=None,
            sending_practice=None,
        )
    ]

    TransferClassifierIO.read_json_files_from_paths = MagicMock(return_value=[an_event])

    MiService.construct_mi_messages_from_mi_events = Mock(return_value=mi_messages)
    MiService.group_mi_messages_by_conversation_id = Mock(return_value=grouped_mi_messages)
    MiService.convert_to_mi_transfers = Mock(return_value=transfers)

    RunnerObservabilityProbe.log_attempting_to_classify = Mock()
    RunnerObservabilityProbe.log_successfully_grouped_mi_messages = Mock()
    RunnerObservabilityProbe.log_successfully_read_mi_events = Mock()
    RunnerObservabilityProbe.log_successfully_constructed_mi_messages = Mock()
    RunnerObservabilityProbe.log_successfully_created_transfers_from_mi_events = Mock()

    MiRunner(build_config()).run()

    MiService.construct_mi_messages_from_mi_events.assert_called_with([an_event])
    MiService.group_mi_messages_by_conversation_id.assert_called_with(mi_messages)
    MiService.convert_to_mi_transfers.assert_called_with(grouped_mi_messages)

    RunnerObservabilityProbe.log_attempting_to_classify.assert_called()
    # RunnerObservabilityProbe.log_successfully_grouped_mi_messages.assert_called()
    RunnerObservabilityProbe.log_successfully_read_mi_events.assert_called()
    RunnerObservabilityProbe.log_successfully_constructed_mi_messages.assert_called()
    RunnerObservabilityProbe.log_successfully_created_transfers_from_mi_events.assert_called()
