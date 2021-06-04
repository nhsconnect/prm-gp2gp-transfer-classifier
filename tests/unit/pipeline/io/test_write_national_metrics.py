from unittest.mock import Mock

from prmdata.domain.data_platform.national_metrics import (
    NationalMetricsPresentation,
    MonthlyNationalMetrics,
    FailedMetrics,
    PendingMetrics,
    PaperFallbackMetrics,
    IntegratedMetrics,
)
from prmdata.pipeline.platform_metrics_calculator.io import PlatformMetricsIO
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.common import a_datetime, a_string


_NATIONAL_METRICS_GENERATED_ON_DATETIME = a_datetime(year=2021, month=1)
_NATIONAL_METRICS_OBJECT = NationalMetricsPresentation(
    generated_on=_NATIONAL_METRICS_GENERATED_ON_DATETIME.isoformat(),
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

_NATIONAL_METRICS_DICT = {
    "generatedOn": _NATIONAL_METRICS_GENERATED_ON_DATETIME.isoformat(),
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


def test_write_national_metrics():
    s3_manager = Mock()
    reporting_window = MonthlyReportingWindow.prior_to(_NATIONAL_METRICS_GENERATED_ON_DATETIME)

    dashboard_data_bucket = a_string()

    metrics_io = PlatformMetricsIO(
        reporting_window=reporting_window,
        s3_data_manager=s3_manager,
        organisation_metadata_bucket=a_string(),
        gp2gp_spine_bucket=a_string(),
        dashboard_data_bucket=dashboard_data_bucket,
    )

    metrics_io.write_national_metrics(_NATIONAL_METRICS_OBJECT)

    expected_national_metrics_dict = _NATIONAL_METRICS_DICT
    expected_path = f"s3://{dashboard_data_bucket}/v2/2020/12/nationalMetrics.json"

    s3_manager.write_json.assert_called_once_with(expected_path, expected_national_metrics_dict)
