from datetime import datetime

from gp2gp.dashboard.models import ServiceDashboardData


def construct_service_dashboard_data() -> ServiceDashboardData:
    return ServiceDashboardData(generated_on=datetime.now())
