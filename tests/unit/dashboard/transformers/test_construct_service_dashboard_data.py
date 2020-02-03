from datetime import datetime
from freezegun import freeze_time

from gp2gp.dashboard.transformers import construct_service_dashboard_data


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42))
def test_dashboard_data_has_correct_generated_on_timestamp():
    expected = datetime(year=2019, month=6, day=2, hour=23, second=42)

    actual = construct_service_dashboard_data()

    assert actual.generated_on == expected
