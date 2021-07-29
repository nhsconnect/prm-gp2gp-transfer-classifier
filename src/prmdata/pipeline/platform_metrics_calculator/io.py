from dataclasses import asdict
from typing import Iterable

from prmdata.domain.data_platform.national_metrics import NationalMetricsPresentation
from prmdata.domain.spine.message import construct_messages_from_splunk_items, Message
from prmdata.utils.io.dictionary import camelize_dict
from prmdata.utils.reporting_window import MonthlyReportingWindow
from prmdata.utils.io.s3 import S3DataManager


class PlatformMetricsIO:
    _SPINE_MESSAGES_VERSION = "v2"
    _SPINE_MESSAGES_PREFIX = "messages"
    _SPINE_MESSAGES_OVERFLOW_PREFIX = "messages-overflow"
    _DASHBOARD_DATA_VERSION = "v4"
    _NATIONAL_METRICS_FILE_NAME = "nationalMetrics.json"

    def __init__(
        self,
        *,
        reporting_window: MonthlyReportingWindow,
        s3_data_manager: S3DataManager,
        gp2gp_spine_bucket: str,
        dashboard_data_bucket: str,
    ):
        self._window = reporting_window
        self._s3_manager = s3_data_manager
        self._gp2gp_spine_bucket = gp2gp_spine_bucket
        self._dashboard_data_bucket = dashboard_data_bucket

    def _read_spine_gzip_csv(self, path: str) -> Iterable[Message]:
        data = self._s3_manager.read_gzip_csv(f"s3://{path}")
        return construct_messages_from_splunk_items(data)

    def _dashboard_data_bucket_s3_path(self, file_name: str) -> str:
        return "/".join(
            [
                self._dashboard_data_bucket,
                self._DASHBOARD_DATA_VERSION,
                self._metric_month_path_fragment(),
                file_name,
            ]
        )

    @staticmethod
    def _create_platform_json_object(platform_data) -> dict:
        content_dict = asdict(platform_data)
        return camelize_dict(content_dict)

    def _metric_month_file_prefix(self) -> str:
        return f"{self._window.metric_year}-{self._window.metric_month}"

    def _overflow_month_file_prefix(self) -> str:
        return f"{self._window.overflow_year}-{self._window.overflow_month}"

    def _metric_month_path_fragment(self) -> str:
        return f"{self._window.metric_year}/{self._window.metric_month}"

    def _overflow_month_path_fragment(self) -> str:
        return f"{self._window.overflow_year}/{self._window.overflow_month}"

    def read_spine_messages(self) -> Iterable[Message]:
        spine_messages_path = "/".join(
            [
                self._gp2gp_spine_bucket,
                self._SPINE_MESSAGES_VERSION,
                self._SPINE_MESSAGES_PREFIX,
                self._metric_month_path_fragment(),
                f"{self._metric_month_file_prefix()}_spine_messages.csv.gz",
            ]
        )
        spine_messages_overflow_path = "/".join(
            [
                self._gp2gp_spine_bucket,
                self._SPINE_MESSAGES_VERSION,
                self._SPINE_MESSAGES_OVERFLOW_PREFIX,
                self._overflow_month_path_fragment(),
                f"{self._overflow_month_file_prefix()}_spine_messages_overflow.csv.gz",
            ]
        )
        yield from self._read_spine_gzip_csv(spine_messages_path)
        yield from self._read_spine_gzip_csv(spine_messages_overflow_path)

    def write_national_metrics(
        self, national_metrics_presentation_data: NationalMetricsPresentation
    ):
        national_metrics_path = self._dashboard_data_bucket_s3_path(
            self._NATIONAL_METRICS_FILE_NAME
        )
        self._s3_manager.write_json(
            f"s3://{national_metrics_path}",
            self._create_platform_json_object(national_metrics_presentation_data),
        )
