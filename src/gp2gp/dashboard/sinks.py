from typing import TextIO
from gp2gp.dashboard.models import ServiceDashboardData
from gp2gp.io.write_as_json import write_as_json


def write_service_dashboard_json(dashboard_data: ServiceDashboardData, outfile: TextIO):
    write_as_json(dashboard_data, outfile)
