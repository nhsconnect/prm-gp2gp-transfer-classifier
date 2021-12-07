import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from dateutil.parser import isoparse

logger = logging.getLogger(__name__)


class MissingEnvironmentVariable(Exception):
    pass


DEFAULT_CUTOFF_DAYS = 14


class EnvConfig:
    def __init__(self, env_vars):
        self._env_vars = env_vars

    def _read_env(self, name, optional, converter=None):
        try:
            env_var = self._env_vars[name]
            if converter:
                return converter(env_var)
            else:
                return env_var
        except KeyError:
            if optional:
                return None
            else:
                raise MissingEnvironmentVariable(
                    f"Expected environment variable {name} was not set, exiting..."
                )

    def read_str(self, name) -> str:
        return self._read_env(name, optional=False)

    def read_optional_str(self, name) -> Optional[str]:
        return self._read_env(name, optional=True)

    def read_optional_timedelta_days(self, name) -> Optional[timedelta]:
        return self._read_env(
            name, optional=True, converter=lambda env_var: timedelta(days=int(env_var))
        )

    def read_datetime(self, name) -> datetime:
        return self._read_env(name, optional=False, converter=isoparse)


@dataclass
class TransferClassifierConfig:
    output_transfer_data_bucket: str
    input_spine_data_bucket: str
    date_anchor: datetime
    build_tag: str
    conversation_cutoff: timedelta
    s3_endpoint_url: Optional[str]

    @classmethod
    def from_environment_variables(cls, env_vars):
        env = EnvConfig(env_vars)
        return TransferClassifierConfig(
            output_transfer_data_bucket=env.read_str("OUTPUT_TRANSFER_DATA_BUCKET"),
            input_spine_data_bucket=env.read_str("INPUT_SPINE_DATA_BUCKET"),
            date_anchor=env.read_datetime("DATE_ANCHOR"),
            build_tag=env.read_str("BUILD_TAG"),
            conversation_cutoff=env.read_optional_timedelta_days("CONVERSATION_CUTOFF_DAYS")
            or timedelta(days=DEFAULT_CUTOFF_DAYS),
            s3_endpoint_url=env.read_optional_str("S3_ENDPOINT_URL"),
        )
