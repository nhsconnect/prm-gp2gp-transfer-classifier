from datetime import timedelta
from unittest.mock import MagicMock, patch

from prmdata.domain.mi.mi_service import (
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadIntegration,
    MiMessagePayloadRegistration,
    MiService,
)
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.io import TransferClassifierIO
from prmdata.pipeline.mi_runner import MiRunner
from prmdata.pipeline.transfer_classifier import RunnerObservabilityProbe
from tests.builders.common import a_datetime


def test_transfer_classifier_spine_runner_abstract_class():
    bucket_name = "test_bucket"
    an_event_id = "1234"
    a_conversation_id = "1111-1111-1111-1111"
    an_event_type = "SOME_EVENT"
    a_transfer_protocol = "GP2GP"
    a_random_datetime = "2022-01-01T00:00:00Z"
    an_integration_status = "merged"
    an_integration_reason = "some reason"
    a_reporting_system_supplier = "ABC"
    a_reporting_practice_ods_code = "ABC123"
    an_event = {
        "eventId": an_event_id,
        "conversationId": a_conversation_id,
        "eventType": an_event_type,
        "transferProtocol": a_transfer_protocol,
        "eventGeneratedDateTime": a_random_datetime,
        "reportingSystemSupplier": a_reporting_system_supplier,
        "reportingPracticeOdsCode": a_reporting_practice_ods_code,
        "transferEventDateTime": a_random_datetime,
        "payload": {
            "integration": {
                "integrationStatus": an_integration_status,
                "reason": an_integration_reason,
            }
        },
    }

    TransferClassifierIO.read_json_files_from_paths = MagicMock(return_value=[an_event])

    config = TransferClassifierConfig(
        input_mi_data_bucket=bucket_name,
        input_spine_data_bucket=bucket_name,
        output_transfer_data_bucket=bucket_name,
        input_ods_metadata_bucket=bucket_name,
        start_datetime=a_datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0),
        end_datetime=a_datetime(year=2022, month=1, day=2, hour=0, minute=0, second=0),
        s3_endpoint_url=None,
        conversation_cutoff=timedelta(days=0),
        build_tag="12345",
        classify_mi_events=True,
    )

    with patch.object(
        MiService, "construct_mi_messages_from_mi_events"
    ) as mock_construct_mi_messages_from_mi_events:
        MiRunner(config).run()
        mock_construct_mi_messages_from_mi_events.assert_called_with([an_event])

    with patch.object(
        MiService, "group_mi_messages_by_conversation_id"
    ) as mock_group_mi_messages_by_conversation_id:
        expected_mi_messages = [
            MiMessage(
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
                        registrationStartedDateTime=None,
                        registrationType=None,
                        requestingPracticeOdsCode=None,
                        sendingPracticeOdsCode=None,
                    ),
                    integration=MiMessagePayloadIntegration(
                        integrationStatus=an_integration_status, reason=an_integration_reason
                    ),
                ),
            )
        ]

        MiRunner(config).run()
        mock_group_mi_messages_by_conversation_id.assert_called_with(expected_mi_messages)

    with patch.object(
        RunnerObservabilityProbe, "log_successfully_read_mi_events"
    ) as mock_log_successfully_read_mi_events:
        MiRunner(config).run()
        mock_log_successfully_read_mi_events.assert_called()

    with patch.object(
        RunnerObservabilityProbe, "log_successfully_constructed_mi_messages"
    ) as mock_log_successfully_constructed_mi_messages:
        MiRunner(config).run()
        mock_log_successfully_constructed_mi_messages.assert_called()

    with patch.object(
        RunnerObservabilityProbe, "log_successfully_grouped_mi_messages"
    ) as mock_log_successfully_grouped_mi_messages:
        MiRunner(config).run()
        mock_log_successfully_grouped_mi_messages.assert_called()
