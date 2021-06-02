from prmdata.domain.ods_portal.models import OrganisationMetadata
from prmdata.utils.date_anchor import DateAnchor
from prmdata.utils.io.s3 import S3DataManager


class PlatformMetricsIO:

    _ORG_METADATA_VERSION = "v2"
    _ORG_METADATA_FILE_NAME = "organisationMetadata.json"

    def __init__(
        self,
        *,
        date_anchor: DateAnchor,
        s3_data_manager: S3DataManager,
        organisation_metadata_bucket: str,
    ):
        self._anchor = date_anchor
        self._s3_manager = s3_data_manager
        self._org_metadata_bucket_name = organisation_metadata_bucket

    def read_ods_metadata(self):
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
