import json
from dataclasses import asdict
from datetime import datetime
from typing import TextIO, List, Mapping

from gp2gp.dashboard.models import ServiceDashboardData


def _camelize(string):
    components = string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def _camelize_dict(obj):
    if isinstance(obj, List):
        return [_camelize_dict(i) for i in obj]
    elif isinstance(obj, Mapping):
        return {_camelize(k): _camelize_dict(v) for k, v in obj.items()}
    return obj


def _serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type {} is not JSON serializable".format(type(obj)))


def _serialize_dashboard_json(dashboard_data):
    data_dict = asdict(dashboard_data)
    camelcase_data_dict = _camelize_dict(data_dict)
    return json.dumps(camelcase_data_dict, default=_serialize_datetime)


def write_service_dashboard_json(dashboard_data: ServiceDashboardData, outfile: TextIO):
    json_output = _serialize_dashboard_json(dashboard_data)
    outfile.write(json_output)
