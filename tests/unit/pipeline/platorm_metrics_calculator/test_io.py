from typing import Iterable
from unittest.mock import Mock, call

from prmdata.domain.ods_portal.models import OrganisationMetadata
from prmdata.domain.spine.message import Message
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.date_anchor import DateAnchor
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
    anchor = DateAnchor(a_datetime())

    organisation_metadata = build_organisation_metadata()
    ods_bucket = "test_ods_bucket"

    metrics_io = PlatformMetricsIO(
        date_anchor=anchor,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=ods_bucket,
        gp2gp_spine_bucket=a_string(),
    )

    s3_manager.read_json.return_value = _org_metadata_as_dict(organisation_metadata)

    expected_path = (
        f"s3://{ods_bucket}/v2/{anchor.current_month_prefix()}/organisationMetadata.json"
    )

    expected_data = organisation_metadata

    actual_data = metrics_io.read_ods_metadata()

    assert actual_data == expected_data

    s3_manager.read_json.assert_called_once_with(expected_path)


def test_read_spine_messages():
    s3_manager = Mock()
    anchor = DateAnchor(a_datetime())

    spine_message_one = build_message()
    spine_message_two = build_message()
    spine_message_three = build_message()
    spine_bucket = "test_spine_bucket"

    metrics_io = PlatformMetricsIO(
        date_anchor=anchor,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=spine_bucket,
    )

    s3_manager.read_gzip_csv.side_effect = [
        iter(_spine_messages_as_dict([spine_message_one, spine_message_two])),
        iter(_spine_messages_as_dict([spine_message_three])),
    ]

    expected_data = [spine_message_one, spine_message_two, spine_message_three]
    expected_path = (
        f"s3://{spine_bucket}/v2/messages/{anchor.previous_month_prefix()}/"
        f"{anchor.previous_month_prefix('-')}_spine_messages.csv.gz"
    )
    expected_overflow_path = (
        f"s3://{spine_bucket}/v2/messages-overflow/{anchor.current_month_prefix()}/"
        f"{anchor.current_month_prefix('-')}_spine_messages_overflow.csv.gz"
    )

    actual_data = list(metrics_io.read_spine_messages())

    assert actual_data == expected_data

    s3_manager.read_gzip_csv.assert_has_calls([call(expected_path), call(expected_overflow_path)])
