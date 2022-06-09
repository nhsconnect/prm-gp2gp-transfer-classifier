from datetime import timedelta

from prmdata.pipeline.config import TransferClassifierConfig
from tests.builders.common import a_datetime, a_string


def build_config(**kwargs) -> TransferClassifierConfig:
    return TransferClassifierConfig(
        input_mi_data_bucket=kwargs.get("input_mi_data_bucket", a_string()),
        input_spine_data_bucket=kwargs.get("input_spine_data_bucket", a_string()),
        output_transfer_data_bucket=kwargs.get("output_transfer_data_bucket", a_string()),
        input_ods_metadata_bucket=kwargs.get("input_ods_metadata_bucket", a_string()),
        start_datetime=kwargs.get(
            "start_datetime", a_datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0)
        ),
        end_datetime=kwargs.get(
            "end_datetime", a_datetime(year=2022, month=1, day=2, hour=0, minute=0, second=0)
        ),
        s3_endpoint_url=kwargs.get("s3_endpoint_url", None),
        conversation_cutoff=kwargs.get("conversation_cutoff", timedelta(0)),
        build_tag=kwargs.get("build_tag", a_string()),
        classify_mi_events=kwargs.get("classify_mi_events", True),
    )
