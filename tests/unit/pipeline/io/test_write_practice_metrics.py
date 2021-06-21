from datetime import datetime
from unittest.mock import Mock

from prmdata.domain.data_platform.practice_metrics import (
    PracticeMetricsPresentation,
    PracticeSummary,
    MonthlyMetrics,
    RequesterMetrics,
    IntegratedPracticeMetrics,
)
from prmdata.domain.ods_portal.models import CcgDetails
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string

_OVERFLOW_MONTH = 1
_OVERFLOW_YEAR = 2021
_METRIC_MONTH = 12
_METRIC_YEAR = 2020

_PRACTICE_METRICS_OBJECT = PracticeMetricsPresentation(
    generated_on=datetime(_OVERFLOW_YEAR, _OVERFLOW_MONTH, 1),
    practices=[
        PracticeSummary(
            ods_code="A12345",
            name="A test GP practice",
            metrics=[
                MonthlyMetrics(
                    year=2021,
                    month=1,
                    requester=RequesterMetrics(
                        integrated=IntegratedPracticeMetrics(
                            transfer_count=1,
                            within_3_days_percentage=100.0,
                            within_8_days_percentage=0.0,
                            beyond_8_days_percentage=0.0,
                        ),
                    ),
                )
            ],
        )
    ],
    ccgs=[CcgDetails(name="A test CCG", ods_code="12A", practices=["A12345"])],
)

_PRACTICE_METRICS_DICT = {
    "generatedOn": datetime(_OVERFLOW_YEAR, _OVERFLOW_MONTH, 1),
    "practices": [
        {
            "odsCode": "A12345",
            "name": "A test GP practice",
            "metrics": [
                {
                    "year": 2021,
                    "month": 1,
                    "requester": {
                        "integrated": {
                            "transferCount": 1,
                            "within3DaysPercentage": 100.0,
                            "within8DaysPercentage": 0.0,
                            "beyond8DaysPercentage": 0.0,
                        }
                    },
                }
            ],
        },
    ],
    "ccgs": [{"name": "A test CCG", "odsCode": "12A", "practices": ["A12345"]}],
}


def test_write_practice_metrics():
    s3_manager = Mock()
    date_anchor = a_datetime(year=_OVERFLOW_YEAR, month=_OVERFLOW_MONTH)
    reporting_window = MonthlyReportingWindow.prior_to(date_anchor)

    dashboard_data_bucket = a_string()

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=a_string(),
        dashboard_data_bucket=dashboard_data_bucket,
    )

    metrics_io.write_practice_metrics(_PRACTICE_METRICS_OBJECT)

    expected_national_metrics_dict = _PRACTICE_METRICS_DICT
    expected_path = (
        f"s3://{dashboard_data_bucket}/v3/{_METRIC_YEAR}/{_METRIC_MONTH}/practiceMetrics.json"
    )

    s3_manager.write_json.assert_called_once_with(expected_path, expected_national_metrics_dict)
