import logging
from typing import Iterable, Dict

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.spine.message import construct_messages_from_splunk_items, Message
from prmdata.domain.datetime import YearMonth
from prmdata.pipeline.arrow import convert_transfers_to_table
from prmdata.utils.io.s3 import S3DataManager

logger = logging.getLogger(__name__)


class TransferClassifierS3UriResolver:

    _SPINE_MESSAGES_VERSION = "v2"
    _TRANSFERS_PARQUET_VERSION = "v6"

    def __init__(self, gp2gp_spine_bucket, transfers_bucket):
        self._gp2gp_spine_bucket = gp2gp_spine_bucket
        self._transfers_bucket = transfers_bucket

    @staticmethod
    def _s3_path(*fragments):
        return "s3://" + "/".join(fragments)

    def _metric_month_path(self, year_month: YearMonth):
        year, month = year_month
        return self._s3_path(
            self._gp2gp_spine_bucket,
            self._SPINE_MESSAGES_VERSION,
            "messages",
            f"{year}/{month}",
            f"{year}-{month}_spine_messages.csv.gz",
        )

    def _overflow_month_path(self, year_month: YearMonth):
        year, month = year_month
        return self._s3_path(
            self._gp2gp_spine_bucket,
            self._SPINE_MESSAGES_VERSION,
            "messages-overflow",
            f"{year}/{month}",
            f"{year}-{month}_spine_messages_overflow.csv.gz",
        )

    def spine_messages(self, metric_month: YearMonth, overflow_month: YearMonth) -> list[str]:
        return [self._metric_month_path(metric_month), self._overflow_month_path(overflow_month)]

    def gp2gp_transfers(self, year_month: YearMonth) -> str:
        year, month = year_month
        return self._s3_path(
            self._transfers_bucket,
            self._TRANSFERS_PARQUET_VERSION,
            f"{year}/{month}",
            f"{year}-{month}-transfers.parquet",
        )


class TransferClassifierIO:
    def __init__(self, s3_data_manager: S3DataManager, output_metadata: Dict[str, str]):
        self._s3_manager = s3_data_manager
        self._output_metadata = output_metadata

    def read_spine_messages(self, s3_uris: list[str]) -> Iterable[Message]:
        for uri in s3_uris:
            data = self._s3_manager.read_gzip_csv(uri)
            yield from construct_messages_from_splunk_items(data)

    def write_transfers(self, transfers: Iterable[Transfer], s3_uri: str):
        self._s3_manager.write_parquet(
            table=convert_transfers_to_table(transfers),
            object_uri=s3_uri,
            metadata=self._output_metadata,
        )
