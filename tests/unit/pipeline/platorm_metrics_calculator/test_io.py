from typing import Iterable
from unittest.mock import Mock, call

from prmdata.domain.data_platform.national_metrics import (
    NationalMetricsPresentation,
    MonthlyNationalMetrics,
    FailedMetrics,
    PendingMetrics,
    PaperFallbackMetrics,
    IntegratedMetrics,
)
from prmdata.domain.ods_portal.models import OrganisationMetadata
from prmdata.domain.spine.message import Message
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string
from tests.builders.ods_portal import build_organisation_metadata
from tests.builders.spine import build_message


def _org_metadata_as_dict(metadata: OrganisationMetadata) -> dict:
    return {
        "generated_on": metadata.generated_on.isoformat(),
        "practices": [
            {"ods_code": practice.ods_code, "name": practice.name, "asids": practice.asids}
            for practice in metadata.practices
        ],
        "ccgs": [{"ods_code": ccg.ods_code, "name": ccg.name} for ccg in metadata.ccgs],
    }


def generate_national_metrics_presentation(reporting_datetime):
    return NationalMetricsPresentation(
        generated_on=reporting_datetime.isoformat(),
        metrics=[
            MonthlyNationalMetrics(
                transfer_count=6,
                integrated=IntegratedMetrics(
                    transfer_percentage=83.33,
                    transfer_count=5,
                    within_3_days=2,
                    within_8_days=2,
                    beyond_8_days=1,
                ),
                failed=FailedMetrics(transfer_count=1, transfer_percentage=16.67),
                pending=PendingMetrics(transfer_count=0, transfer_percentage=0.0),
                paper_fallback=PaperFallbackMetrics(transfer_count=2, transfer_percentage=33.33),
                year=2019,
                month=12,
            )
        ],
    )


def _national_metrics_as_dict(reporting_datetime):
    return {
        "generatedOn": reporting_datetime.isoformat(),
        "metrics": [
            {
                "transferCount": 6,
                "integrated": {
                    "transferPercentage": 83.33,
                    "transferCount": 5,
                    "within3Days": 2,
                    "within8Days": 2,
                    "beyond8Days": 1,
                },
                "failed": {"transferCount": 1, "transferPercentage": 16.67},
                "pending": {"transferCount": 0, "transferPercentage": 0.0},
                "paperFallback": {"transferCount": 2, "transferPercentage": 33.33},
                "year": 2019,
                "month": 12,
            }
        ],
    }


def _spine_messages_as_dict(spine_messages: Iterable[Message]) -> Iterable[dict]:
    return [
        {
            "_time": message.time.isoformat(),
            "conversationID": message.conversation_id,
            "GUID": message.guid,
            "interactionID": message.interaction_id,
            "messageSender": message.from_party_asid,
            "messageRecipient": message.to_party_asid,
            "messageRef": message.message_ref,
            "fromSystem": message.from_system,
            "toSystem": message.to_system,
            "jdiEvent": "NONE" if message.error_code is None else str(message.error_code),
        }
        for message in spine_messages
    ]


def test_read_organisation_metadata():
    s3_manager = Mock()
    reporting_window = MonthlyReportingWindow.prior_to(a_datetime(year=2021, month=2))

    organisation_metadata = build_organisation_metadata()
    ods_bucket = "test_ods_bucket"

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=ods_bucket,
        gp2gp_spine_bucket=a_string(),
        dashboard_data_bucket=a_string(),
    )

    s3_manager.read_json.return_value = _org_metadata_as_dict(organisation_metadata)

    expected_path = f"s3://{ods_bucket}/v2/2021/2/organisationMetadata.json"

    expected_data = organisation_metadata

    actual_data = metrics_io.read_ods_metadata()

    assert actual_data == expected_data

    s3_manager.read_json.assert_called_once_with(expected_path)


def test_read_spine_messages():
    s3_manager = Mock()
    reporting_window = MonthlyReportingWindow.prior_to(a_datetime(year=2021, month=2))

    spine_message_one = build_message()
    spine_message_two = build_message()
    spine_message_three = build_message()
    spine_bucket = "test_spine_bucket"

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=spine_bucket,
        dashboard_data_bucket=a_string(),
    )

    s3_manager.read_gzip_csv.side_effect = [
        iter(_spine_messages_as_dict([spine_message_one, spine_message_two])),
        iter(_spine_messages_as_dict([spine_message_three])),
    ]

    expected_data = [spine_message_one, spine_message_two, spine_message_three]
    expected_path = f"s3://{spine_bucket}/v2/messages/2021/1/2021-1_spine_messages.csv.gz"
    expected_overflow_path = (
        f"s3://{spine_bucket}/v2/messages-overflow/2021/2/2021-2_spine_messages_overflow.csv.gz"
    )

    actual_data = list(metrics_io.read_spine_messages())

    assert actual_data == expected_data

    s3_manager.read_gzip_csv.assert_has_calls([call(expected_path), call(expected_overflow_path)])


def test_write_national_metrics():
    s3_manager = Mock()
    reporting_window_datetime = a_datetime(year=2021, month=1)
    reporting_window = MonthlyReportingWindow.prior_to(reporting_window_datetime)

    dashboard_data_bucket = a_string()

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=a_string(),
        dashboard_data_bucket=dashboard_data_bucket,
    )

    national_metrics_presentation = generate_national_metrics_presentation(
        reporting_window_datetime
    )

    metrics_io.write_national_metrics(national_metrics_presentation)

    expected_national_metrics_dict = _national_metrics_as_dict(reporting_window_datetime)
    expected_path = f"s3://{dashboard_data_bucket}/v2/2020/12/nationalMetrics.json"

    s3_manager.write_json.assert_called_once_with(expected_path, expected_national_metrics_dict)
