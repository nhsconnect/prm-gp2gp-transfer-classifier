import logging
from typing import Iterable

from prmdata.domain.spine.message import construct_messages_from_splunk_items, Message
from prmdata.domain.datetime import MonthlyReportingWindow
from prmdata.utils.io.s3 import S3DataManager

logger = logging.getLogger(__name__)


class TransferClassifierS3UriResolver:

    _SPINE_MESSAGES_VERSION = "v2"
    _TRANSFERS_PARQUET_VERSION = "v5"

    def __init__(self, gp2gp_spine_bucket, transfers_bucket):
        self._gp2gp_spine_bucket = gp2gp_spine_bucket
        self._transfers_bucket = transfers_bucket

    @staticmethod
    def _s3_path(*fragments):
        return "s3://" + "/".join(fragments)

    def spine_messages(self, window: MonthlyReportingWindow) -> list[str]:
        spine_messages_path = self._s3_path(
            self._gp2gp_spine_bucket,
            self._SPINE_MESSAGES_VERSION,
            "messages",
            f"{window.metric_year}/{window.metric_month}",
            f"{window.metric_year}-{window.metric_month}_spine_messages.csv.gz",
        )
        spine_messages_overflow_path = self._s3_path(
            self._gp2gp_spine_bucket,
            self._SPINE_MESSAGES_VERSION,
            "messages-overflow",
            f"{window.overflow_year}/{window.overflow_month}",
            f"{window.overflow_year}-{window.overflow_month}_spine_messages_overflow.csv.gz",
        )

        return [spine_messages_path, spine_messages_overflow_path]

    def gp2gp_transfers(self, window: MonthlyReportingWindow) -> str:
        return self._s3_path(
            self._transfers_bucket,
            self._TRANSFERS_PARQUET_VERSION,
            f"{window.metric_year}/{window.metric_month}",
            f"{window.metric_year}-{window.metric_month}-transfers.parquet",
        )


class TransferClassifierIO:
    _SPINE_MESSAGES_VERSION = "v2"
    _SPINE_MESSAGES_PREFIX = "messages"
    _SPINE_MESSAGES_OVERFLOW_PREFIX = "messages-overflow"

    def __init__(self, s3_data_manager: S3DataManager):
        self._s3_manager = s3_data_manager

    def read_spine_messages(self, s3_uris: list[str]) -> Iterable[Message]:
        for uri in s3_uris:
            data = self._s3_manager.read_gzip_csv(uri)
            yield from construct_messages_from_splunk_items(data)
