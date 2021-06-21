from datetime import datetime
from unittest.mock import Mock

from prmdata.domain.ods_portal.models import OrganisationMetadata, PracticeDetails, CcgDetails
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string

_OVERFLOW_MONTH = 1
_OVERFLOW_YEAR = 2021

_ORGANISATION_METADATA_OBJECT = OrganisationMetadata(
    generated_on=datetime(_OVERFLOW_YEAR, _OVERFLOW_MONTH, 1),
    practices=[PracticeDetails(ods_code="ABC", name="A Practice", asids=["123"])],
    ccgs=[CcgDetails(ods_code="XYZ", name="A CCG", practices=["ABC"])],
)
_ORGANISATION_METADATA_DICT = {
    "generated_on": "2021-01-01T00:00:00",
    "practices": [{"ods_code": "ABC", "name": "A Practice", "asids": ["123"]}],
    "ccgs": [{"ods_code": "XYZ", "name": "A CCG", "practices": ["ABC"]}],
}


def test_read_organisation_metadata():
    s3_manager = Mock()
    date_anchor = a_datetime(year=_OVERFLOW_YEAR, month=_OVERFLOW_MONTH)
    reporting_window = MonthlyReportingWindow.prior_to(date_anchor)

    ods_bucket = "test_ods_bucket"

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=ods_bucket,
        gp2gp_spine_bucket=a_string(),
        dashboard_data_bucket=a_string(),
    )

    s3_manager.read_json.return_value = _ORGANISATION_METADATA_DICT

    expected_path = (
        f"s3://{ods_bucket}/v2/{_OVERFLOW_YEAR}/{_OVERFLOW_MONTH}/organisationMetadata.json"
    )

    expected_data = _ORGANISATION_METADATA_OBJECT

    actual_data = metrics_io.read_ods_metadata()

    assert actual_data == expected_data

    s3_manager.read_json.assert_called_once_with(expected_path)
