import json
from datetime import datetime
from typing import TextIO

from gp2gp.dashboard.models import ServiceDashboardData


def _serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type {} is not JSON serializable".format(type(obj)))


def _serialize_dashboard_json(obj):
    return json.dumps(obj, default=_serialize_datetime)


def write_service_dashboard_json(dashboard_data: ServiceDashboardData, outfile: TextIO):
    output = {"generatedOn": dashboard_data.generated_on, "practices": []}
    json_output = _serialize_dashboard_json(output)
    outfile.write(json_output)
