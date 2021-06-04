from datetime import datetime
from unittest.mock import Mock

from prmdata.domain.data_platform.organisation_metadata import (
    OrganisationMetadataPresentation,
    OrganisationDetails,
)
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string


_OVERFLOW_MONTH = 1
_OVERFLOW_YEAR = 2021
_METRIC_MONTH = 12
_METRIC_YEAR = 2020

_ORGANISATION_METADATA_OBJECT = OrganisationMetadataPresentation(
    generated_on=datetime(_OVERFLOW_YEAR, _OVERFLOW_MONTH, 1),
    practices=[OrganisationDetails(ods_code="A1234", name="Test GP Practice")],
    ccgs=[OrganisationDetails(ods_code="11A", name="Test CCG")],
)
_ORGANISATION_METADATA_DICT = {
    "generatedOn": datetime(_OVERFLOW_YEAR, _OVERFLOW_MONTH, 1),
    "practices": [
        {"odsCode": "A1234", "name": "Test GP Practice"},
    ],
    "ccgs": [
        {"odsCode": "11A", "name": "Test CCG"},
    ],
}


def test_write_organisation_metadata():
    s3_manager = Mock()
    date_anchor = a_datetime(year=_OVERFLOW_YEAR, month=_OVERFLOW_MONTH)
    reporting_window = MonthlyReportingWindow.prior_to(date_anchor)

    metrics_bucket = a_string()

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=a_string(),
        dashboard_data_bucket=metrics_bucket,
    )

    metrics_io.write_organisation_metadata(_ORGANISATION_METADATA_OBJECT)

    expected_organisation_metadata_dict = _ORGANISATION_METADATA_DICT
    expected_path = (
        f"s3://{metrics_bucket}/v2/{_METRIC_YEAR}/{_METRIC_MONTH}/organisationMetadata.json"
    )

    s3_manager.write_json.assert_called_once_with(
        expected_path, expected_organisation_metadata_dict
    )
