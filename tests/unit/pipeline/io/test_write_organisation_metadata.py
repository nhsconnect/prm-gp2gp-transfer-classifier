from unittest.mock import Mock

from prmdata.domain.data_platform.organisation_metadata import (
    OrganisationMetadataPresentation,
    OrganisationDetails,
)
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string


_ORGANISATION_METADATA_GENERATED_ON_DATETIME = a_datetime(year=2021, month=1)
_ORGANISATION_METADATA_OBJECT = OrganisationMetadataPresentation(
    generated_on=_ORGANISATION_METADATA_GENERATED_ON_DATETIME.isoformat(),
    practices=[OrganisationDetails(ods_code="A1234", name="Test GP Practice")],
    ccgs=[OrganisationDetails(ods_code="11A", name="Test CCG")],
)
_ORGANISATION_METADATA_DICT = {
    "generatedOn": _ORGANISATION_METADATA_GENERATED_ON_DATETIME.isoformat(),
    "practices": [
        {"odsCode": "A1234", "name": "Test GP Practice"},
    ],
    "ccgs": [
        {"odsCode": "11A", "name": "Test CCG"},
    ],
}


def test_write_organisation_metadata():
    s3_manager = Mock()
    reporting_window = MonthlyReportingWindow.prior_to(_ORGANISATION_METADATA_GENERATED_ON_DATETIME)

    output_dashboard_data_bucket = a_string()

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=a_string(),
        dashboard_data_bucket=output_dashboard_data_bucket,
    )

    metrics_io.write_organisation_metadata(_ORGANISATION_METADATA_OBJECT)

    expected_organisation_metadata_dict = _ORGANISATION_METADATA_DICT
    expected_path = f"s3://{output_dashboard_data_bucket}/v2/2020/12/organisationMetadata.json"

    s3_manager.write_json.assert_called_once_with(
        expected_path, expected_organisation_metadata_dict
    )
