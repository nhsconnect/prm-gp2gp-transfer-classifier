from typing import Iterable

from prmdata.domain.ods_portal.models import OrganisationMetadata
from prmdata.domain.spine.message import construct_messages_from_splunk_items, Message
from prmdata.utils.date_anchor import DateAnchor
from prmdata.utils.io.s3 import S3DataManager


class PlatformMetricsIO:

    _ORG_METADATA_VERSION = "v2"
    _ORG_METADATA_FILE_NAME = "organisationMetadata.json"
    _SPINE_MESSAGES_VERSION = "v2"
    _SPINE_MESSAGES_PREFIX = "messages"
    _SPINE_MESSAGES_OVERFLOW_PREFIX = "messages-overflow"

    def __init__(
        self,
        *,
        date_anchor: DateAnchor,
        s3_data_manager: S3DataManager,
        organisation_metadata_bucket: str,
        gp2gp_spine_bucket: str,
    ):
        self._anchor = date_anchor
        self._s3_manager = s3_data_manager
        self._org_metadata_bucket_name = organisation_metadata_bucket
        self._gp2gp_spine_bucket = gp2gp_spine_bucket

    def read_ods_metadata(self) -> OrganisationMetadata:
        ods_metadata_s3_path = "/".join(
            [
                self._org_metadata_bucket_name,
                self._ORG_METADATA_VERSION,
                self._anchor.current_month_prefix(),
                self._ORG_METADATA_FILE_NAME,
            ]
        )

        ods_metadata_dict = self._s3_manager.read_json(f"s3://{ods_metadata_s3_path}")
        return OrganisationMetadata.from_dict(ods_metadata_dict)

    def _read_spine_gzip_csv(self, path):
        data = self._s3_manager.read_gzip_csv(f"s3://{path}")
        return construct_messages_from_splunk_items(data)

    def read_spine_messages(self) -> Iterable[Message]:
        spine_messages_path = "/".join(
            [
                self._gp2gp_spine_bucket,
                self._SPINE_MESSAGES_VERSION,
                self._SPINE_MESSAGES_PREFIX,
                self._anchor.previous_month_prefix(),
                f"{self._anchor.previous_month_prefix('-')}_spine_messages.csv.gz",
            ]
        )
        spine_messages_overflow_path = "/".join(
            [
                self._gp2gp_spine_bucket,
                self._SPINE_MESSAGES_VERSION,
                self._SPINE_MESSAGES_OVERFLOW_PREFIX,
                self._anchor.current_month_prefix(),
                f"{self._anchor.current_month_prefix('-')}_spine_messages_overflow.csv.gz",
            ]
        )
        yield from self._read_spine_gzip_csv(spine_messages_path)
        yield from self._read_spine_gzip_csv(spine_messages_overflow_path)
