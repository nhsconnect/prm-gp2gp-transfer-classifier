from datetime import timedelta
from unittest.mock import patch

import boto3
from moto import mock_s3

from prmdata.domain.mi.mi_service import (
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadRegistration,
    MiService,
)
from prmdata.pipeline.config import TransferClassifierConfig
from prmdata.pipeline.mi_runner import MiRunner
from prmdata.pipeline.transfer_classifier import RunnerObservabilityProbe
from tests.builders.common import a_datetime
from tests.unit.utils.io.s3 import MOTO_MOCK_REGION


@mock_s3
def test_transfer_classifier_spine_runner_abstract_class():
    conn = boto3.resource("s3", region_name=MOTO_MOCK_REGION)
    conn.create_bucket(Bucket="test_bucket")

    bucket_name = "test_bucket"
    bucket = conn.create_bucket(Bucket=bucket_name)
    bucket.Object("v1/2022/01/01/event1.json").put(
        Body=b'{"eventId": "1234",'
        b'"conversationId": "1111-1111-1111-1111",'
        b'"eventType": "SOME_EVENT",'
        b'"transferProtocol": "GP2GP",'
        b'"eventGeneratedDateTime": "2022-01-01T00:00:00Z",'
        b'"reportingSystemSupplier": "ABC",'
        b'"reportingPracticeOdsCode": "ABC123",'
        b'"transferEventDateTime": "2022-01-01T00:00:00Z",'
        b'"payload": {"registration": {}}}'
    )

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

    expected_mi_event_dict = [
        {
            "eventId": "1234",
            "conversationId": "1111-1111-1111-1111",
            "eventType": "SOME_EVENT",
            "transferProtocol": "GP2GP",
            "eventGeneratedDateTime": "2022-01-01T00:00:00Z",
            "reportingSystemSupplier": "ABC",
            "reportingPracticeOdsCode": "ABC123",
            "transferEventDateTime": "2022-01-01T00:00:00Z",
            "payload": {"registration": {}},
        }
    ]

    expected_mi_messages = [
        MiMessage(
            conversation_id="1111-1111-1111-1111",
            event_id="1234",
            event_type="SOME_EVENT",
            transfer_protocol="GP2GP",
            event_generated_datetime="2022-01-01T00:00:00Z",
            reporting_system_supplier="ABC",
            reporting_practice_ods_code="ABC123",
            transfer_event_datetime="2022-01-01T00:00:00Z",
            payload=MiMessagePayload(
                registration=MiMessagePayloadRegistration(
                    registrationStartedDateTime=None,
                    registrationType=None,
                    requestingPracticeOdsCode=None,
                    sendingPracticeOdsCode=None,
                )
            ),
        )
    ]

    with patch.object(
        MiService, "construct_mi_messages_from_mi_events"
    ) as mock_construct_mi_messages_from_mi_events:
        MiRunner(config).run()
        mock_construct_mi_messages_from_mi_events.assert_called_with(expected_mi_event_dict)

    with patch.object(
        MiService, "group_mi_messages_by_conversation_id"
    ) as mock_group_mi_messages_by_conversation_id:
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
