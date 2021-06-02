from unittest.mock import Mock

from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.date_anchor import DateAnchor
from tests.builders.common import a_datetime
from tests.builders.ods_portal import build_organisation_metadata


def _org_metadata_as_json(metadata):
    return {
        "generated_on": metadata.generated_on.isoformat(),
        "practices": [
            {"ods_code": practice.ods_code, "name": practice.name, "asids": practice.asids}
            for practice in metadata.practices
        ],
        "ccgs": [{"ods_code": ccg.ods_code, "name": ccg.name} for ccg in metadata.ccgs],
    }


def test_read_organisation_metadata():
    s3_manager = Mock()
    anchor = DateAnchor(a_datetime())

    organisation_metadata = build_organisation_metadata()
    ods_bucket = "test_ods_bucket"

    metrics_io = PlatformMetricsIO(
        date_anchor=anchor, s3_data_manager=s3_manager, organisation_metadata_bucket=ods_bucket
    )

    s3_manager.read_json.return_value = _org_metadata_as_json(organisation_metadata)

    expected_path = (
        f"s3://{ods_bucket}/v2/{anchor.current_month_prefix()}/organisationMetadata.json"
    )

    expected_data = organisation_metadata

    actual_data = metrics_io.read_ods_metadata()

    assert actual_data == expected_data

    s3_manager.read_json.assert_called_once_with(expected_path)
