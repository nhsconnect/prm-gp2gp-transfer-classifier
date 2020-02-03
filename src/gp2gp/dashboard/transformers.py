from datetime import datetime

from gp2gp.dashboard.models import ServiceDashboardData


def construct_service_dashboard_data() -> ServiceDashboardData:
    return ServiceDashboardData(generated_on=datetime(year=2019, month=6, day=2, hour=23, second=42))
