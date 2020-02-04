import json
from datetime import datetime
from io import StringIO

from gp2gp.dashboard.models import ServiceDashboardData
from gp2gp.dashboard.sinks import write_service_dashboard_json


def test_write_service_dashboard_json_correctly_serializes_generated_on():
    dashboard_data = ServiceDashboardData(
        generated_on=datetime(
            year=2020, month=1, day=1, hour=11, minute=0, second=7, microsecond=1
        ),
        practices=[],
    )
    outfile = StringIO()

    expected = json.dumps({"generatedOn": "2020-01-01T11:00:07.000001", "practices": []})

    write_service_dashboard_json(dashboard_data, outfile)

    outfile.seek(0)
    actual = outfile.read()

    assert actual == expected
