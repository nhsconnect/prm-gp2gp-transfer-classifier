import logging
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.ods_portal.organisation_metadata_monthly import OrganisationMetadataMonthly
from prmdata.domain.reporting_window import ReportingWindow
from prmdata.domain.spine.message import Message, construct_messages_from_splunk_items
from prmdata.pipeline.arrow import convert_transfers_to_table
from prmdata.utils.add_leading_zero import add_leading_zero
from prmdata.utils.input_output.s3 import S3DataManager

logger = logging.getLogger(__name__)


class TransferClassifierS3UriResolver:
    _SPINE_MESSAGES_VERSION = "v3"
    _ODS_METADATA_VERSION = "v3"
    _TRANSFERS_PARQUET_VERSION = "v7"

    def __init__(self, gp2gp_spine_bucket: str, transfers_bucket: str, ods_metadata_bucket: str):
        self._gp2gp_spine_bucket = gp2gp_spine_bucket
        self._transfers_bucket = transfers_bucket
        self._ods_metadata_bucket = ods_metadata_bucket

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

    def ods_metadata(self, reporting_window: ReportingWindow) -> List[str]:
        return [
            self._s3_path(
                self._ods_metadata_bucket,
                self._ODS_METADATA_VERSION,
                f"{date.year}/{date.month}",
                "organisationMetadata.json",
            )
            for date in reporting_window.get_dates()
        ]

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


class TransferClassifierIO:
    def __init__(self, s3_data_manager: S3DataManager):
        self._s3_manager = s3_data_manager

    def read_spine_messages(self, s3_uris: List[str]) -> Iterator[Message]:
        for uri in s3_uris:
            data = self._s3_manager.read_gzip_csv(uri)
            yield from construct_messages_from_splunk_items(data)

    def write_transfers(self, transfers: Iterable[Transfer], s3_uri: str, metadata: Dict[str, str]):
        self._s3_manager.write_parquet(
            table=convert_transfers_to_table(transfers),
            object_uri=s3_uri,
            metadata=metadata,
        )

    def _read_ods_metadata(self, s3_uris: List[str]) -> Iterator[Dict]:
        for uri in s3_uris:
            yield self._s3_manager.read_json(uri)

    def read_ods_metadata_files(self, s3_uris: List[str]) -> OrganisationMetadataMonthly:
        return OrganisationMetadataMonthly.from_list(self._read_ods_metadata(s3_uris))
