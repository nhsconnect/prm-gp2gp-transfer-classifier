from datetime import datetime, timedelta
from typing import List, Optional

from dateutil.relativedelta import relativedelta

from prmdata.domain.reporting_window import ReportingWindow
from prmdata.utils.add_leading_zero import add_leading_zero


class TransferClassifierS3UriResolver:
    _SPINE_MESSAGES_VERSION = "v3"
    _ODS_METADATA_VERSION = "v5"
    _MI_EVENTS_VERSION = "v1"
    _TRANSFERS_PARQUET_VERSION = "v11"

    def __init__(
        self,
        gp2gp_spine_bucket: str,
        transfers_bucket: str,
        ods_metadata_bucket: str,
        mi_bucket: Optional[str] = None,
    ):
        self._gp2gp_spine_bucket = gp2gp_spine_bucket
        self._transfers_bucket = transfers_bucket
        self._ods_metadata_bucket = ods_metadata_bucket
        self._mi_bucket = mi_bucket

    @staticmethod
    def _s3_path(*fragments):
        return "s3://" + "/".join(fragments)

    @staticmethod
    def _spine_message_filename(date: datetime) -> str:
        year = add_leading_zero(date.year)
        month = add_leading_zero(date.month)
        day = add_leading_zero(date.day)
        return f"{year}-{month}-{day}_spine_messages.csv.gz"

    def spine_messages(self, reporting_window: ReportingWindow) -> List[str]:
        dates = reporting_window.get_dates() + reporting_window.get_overflow_dates()
        return [
            self._s3_path(
                self._gp2gp_spine_bucket,
                self._SPINE_MESSAGES_VERSION,
                f"{add_leading_zero(date.year)}",
                f"{add_leading_zero(date.month)}",
                f"{add_leading_zero(date.day)}",
                self._spine_message_filename(date),
            )
            for date in dates
        ]

    def ods_metadata(self, reporting_window_dates: List[datetime]) -> List[str]:
        reporting_window_months = [(date.year, date.month) for date in reporting_window_dates]
        deduplicated_reporting_months = list(dict.fromkeys(reporting_window_months))
        return [
            self._s3_path(
                self._ods_metadata_bucket,
                self._ODS_METADATA_VERSION,
                f"{year}/{month}",
                "organisationMetadata.json",
            )
            for (year, month) in deduplicated_reporting_months
        ]

    def ods_metadata_using_previous_month(
        self, reporting_window_dates: List[datetime]
    ) -> List[str]:
        if len(reporting_window_dates) == 1:
            reporting_window_for_previous_month = [
                date - relativedelta(months=1) for date in reporting_window_dates
            ]
            return self.ods_metadata(reporting_window_for_previous_month)
        else:
            return self.ods_metadata(reporting_window_dates)[:-1]

    def gp2gp_transfers(self, daily_start_datetime: datetime, cutoff: timedelta) -> str:
        year = add_leading_zero(daily_start_datetime.year)
        month = add_leading_zero(daily_start_datetime.month)
        day = add_leading_zero(daily_start_datetime.day)
        return self._s3_path(
            self._transfers_bucket,
            self._TRANSFERS_PARQUET_VERSION,
            f"cutoff-{cutoff.days}",
            f"{year}/{month}/{day}",
            f"{year}-{month}-{day}-transfers.parquet",
        )

    def mi_events(self, reporting_window: ReportingWindow) -> List[str]:
        dates = reporting_window.get_dates() + reporting_window.get_overflow_dates()
        return [
            self._s3_path(
                self._mi_bucket,
                self._MI_EVENTS_VERSION,
                f"{add_leading_zero(date.year)}",
                f"{add_leading_zero(date.month)}",
                f"{add_leading_zero(date.day)}",
            )
            for date in dates
        ]
