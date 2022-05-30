import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from dateutil.parser import isoparse

logger = logging.getLogger(__name__)


class MissingEnvironmentVariable(Exception):
    pass


class EnvConfig:
    def __init__(self, env_vars):
        self._env_vars = env_vars

    def _read_env(self, name: str, optional: bool, converter=None, default=None):
        try:
            env_var = self._env_vars[name]
            if converter:
                return converter(env_var)
            else:
                return env_var
        except KeyError:
            if optional:
                return default
            else:
                raise MissingEnvironmentVariable(
                    f"Expected environment variable {name} was not set, exiting..."
                )

    def read_str(self, name: str) -> str:
        return self._read_env(name, optional=False)

    def read_optional_str(self, name: str) -> Optional[str]:
        return self._read_env(name, optional=True)

    def read_optional_int(self, name: str, default: int) -> int:
        return self._read_env(name, optional=True, converter=int, default=default)

    def read_optional_timedelta_days(self, name: str, default: timedelta) -> timedelta:
        return self._read_env(
            name,
            optional=True,
            converter=lambda env_var: timedelta(days=int(env_var)),
            default=default,
        )

    def read_optional_datetime(self, name: str) -> datetime:
        return self._read_env(name, optional=True, converter=isoparse)

    def read_optional_bool(self, name: str, default: bool) -> bool:
        return self._read_env(
            name, optional=True, converter=lambda string: string.lower() == "true", default=default
        )


@dataclass
class TransferClassifierConfig:
    output_transfer_data_bucket: str
    input_spine_data_bucket: str
    input_ods_metadata_bucket: str
    input_mi_data_bucket: Optional[str]
    start_datetime: Optional[datetime]
    end_datetime: Optional[datetime]
    build_tag: str
    conversation_cutoff: timedelta
    s3_endpoint_url: Optional[str]
    classify_mi_events: Optional[bool]

    def __str__(self):
        return str(self.__dict__)

    @classmethod
    def from_environment_variables(cls, env_vars: object):
        env = EnvConfig(env_vars)
        return TransferClassifierConfig(
            output_transfer_data_bucket=env.read_str("OUTPUT_TRANSFER_DATA_BUCKET"),
            input_spine_data_bucket=env.read_str("INPUT_SPINE_DATA_BUCKET"),
            input_ods_metadata_bucket=env.read_str("INPUT_ODS_METADATA_BUCKET"),
            input_mi_data_bucket=env.read_optional_str("INPUT_MI_DATA_BUCKET"),
            start_datetime=env.read_optional_datetime("START_DATETIME"),
            end_datetime=env.read_optional_datetime("END_DATETIME"),
            build_tag=env.read_str("BUILD_TAG"),
            conversation_cutoff=env.read_optional_timedelta_days(
                "CONVERSATION_CUTOFF_DAYS", timedelta(days=0)
            ),
            s3_endpoint_url=env.read_optional_str("S3_ENDPOINT_URL"),
            classify_mi_events=env.read_optional_bool("CLASSIFY_MI_EVENTS", default=False),
        )
