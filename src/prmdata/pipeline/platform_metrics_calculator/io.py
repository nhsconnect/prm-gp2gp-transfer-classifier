from prmdata.domain.ods_portal.models import construct_organisation_metadata_from_dict


class PlatformMetricsIO:

    _ORG_METADATA_VERSION = "v2"
    _ORG_METADATA_FILE_NAME = "organisationMetadata.json"

    def __init__(self, *, date_anchor, s3_data_manager, organisation_metadata_bucket):
        self._anchor = date_anchor
        self._s3_manager = s3_data_manager
        self._org_metadata_bucket_name = organisation_metadata_bucket

    def read_ods_metadata(self):
        ods_metadata_s3_path = "/".join(
            [
                self._org_metadata_bucket_name,
                self._ORG_METADATA_VERSION,
                self._anchor.current_year,
                self._anchor.current_month,
                self._ORG_METADATA_FILE_NAME,
            ]
        )

        ods_metadata_dict = self._s3_manager.read_json(f"s3://{ods_metadata_s3_path}")
        return construct_organisation_metadata_from_dict(ods_metadata_dict)
