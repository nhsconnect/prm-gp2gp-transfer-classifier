import logging
from typing import Dict, Iterable, Iterator, List

from prmdata.domain.gp2gp.transfer import Transfer
from prmdata.domain.ods_portal.organisation_metadata_monthly import OrganisationMetadataMonthly
from prmdata.domain.spine.message import Message, construct_messages_from_splunk_items
from prmdata.pipeline.arrow import convert_transfers_to_table
from prmdata.utils.input_output.s3 import S3DataManager

logger = logging.getLogger(__name__)


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

    def read_json_files_from_paths(self, s3_paths: List[str]) -> List[dict]:
        return self._s3_manager.read_json_files_from_paths(s3_paths)
