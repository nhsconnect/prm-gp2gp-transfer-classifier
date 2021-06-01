import typing
from datetime import datetime
from dataclasses import dataclass, MISSING, fields
import logging

from typing import Optional

logger = logging.getLogger(__name__)


class MissingEnvironmentVariable(Exception):
    pass


def _convert_env_value(env_value, config_type):
    if config_type == typing.Optional[int]:
        return int(env_value)
    return env_value


def _read_env(field, env_vars):
    env_var = field.name.upper()
    if env_var in env_vars:
        env_value = env_vars[env_var]
        return _convert_env_value(env_value, field.type)
    elif field.default != MISSING:
        return field.default
    else:
        raise MissingEnvironmentVariable(
            f"Expected environment variable {env_var} was not set, exiting..."
        )


@dataclass
class DataPipelineConfig:
    output_transfer_data_bucket: str
    input_transfer_data_bucket: str
    organisation_list_bucket: str
    month: Optional[int] = datetime.utcnow().month
    year: Optional[int] = datetime.utcnow().year
    s3_endpoint_url: Optional[str] = None

    @classmethod
    def from_environment_variables(cls, env_vars):
        return cls(**{field.name: _read_env(field, env_vars) for field in fields(cls)})
