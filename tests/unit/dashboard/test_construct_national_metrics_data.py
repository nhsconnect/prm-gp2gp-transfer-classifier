from datetime import datetime

from dateutil.tz import tzutc
from freezegun import freeze_time

from gp2gp.dashboard.nationalData import construct_national_data_platform_data
from gp2gp.service.nationalMetricsByMonth import NationalMetricsByMonth, IntegratedMetrics
from tests.builders.common import an_integer


def build_national_metrics_by_month(**kwargs):
    transfer_count = kwargs.get("transfer_count", an_integer())
    return NationalMetricsByMonth(
        transfer_count=kwargs.get("transfer_count", an_integer()),
        integrated=IntegratedMetrics(
            transfer_count=kwargs.get("integrated_transfer_count", an_integer(0, transfer_count)),
            within_3_days=an_integer(),
            within_8_days=an_integer(),
            beyond_8_days=an_integer(),
        ),
    )


@freeze_time(datetime(year=2019, month=6, day=2, hour=23, second=42), tz_offset=0)
def test_has_correct_generated_on_given_time():
    expected_generated_on = datetime(year=2019, month=6, day=2, hour=23, second=42, tzinfo=tzutc())
    national_metrics_by_month = build_national_metrics_by_month()
    actual = construct_national_data_platform_data(national_metrics_by_month)

    assert actual.generated_on == expected_generated_on


def test_has_transfer_count_of_all_transfers():
    expected_transfer_count = an_integer(2, 7)
    national_metrics_by_month = build_national_metrics_by_month(
        transfer_count=expected_transfer_count
    )
    actual = construct_national_data_platform_data(national_metrics_by_month)

    assert actual.metrics.transfer_count == expected_transfer_count


def test_has_integrated_transfer_count():
    expected_integrated_transfer_count = an_integer(2, 7)
    national_metrics_by_month = build_national_metrics_by_month(
        integrated_transfer_count=expected_integrated_transfer_count
    )
    actual = construct_national_data_platform_data(national_metrics_by_month)

    assert actual.metrics.integrated.transfer_count == expected_integrated_transfer_count
