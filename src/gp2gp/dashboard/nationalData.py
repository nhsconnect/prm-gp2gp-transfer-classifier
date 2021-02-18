from dataclasses import dataclass
from datetime import datetime
from dateutil.tz import tzutc

from gp2gp.service.nationalMetricsByMonth import NationalMetricsByMonth


@dataclass
class IntegratedData:
    transfer_count: int
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


@dataclass
class NationalData:
    transfer_count: int
    integrated: IntegratedData


@dataclass
class NationalDataPlatformData:
    generated_on: datetime
    metrics: NationalData


def construct_national_data_platform_data(
    national_metrics_by_month: NationalMetricsByMonth,
) -> NationalDataPlatformData:
    current_datetime = datetime.now(tzutc())

    integrated_data = IntegratedData(
        transfer_count=national_metrics_by_month.integrated.transfer_count,
        within_3_days=national_metrics_by_month.integrated.within_3_days,
        within_8_days=national_metrics_by_month.integrated.within_8_days,
        beyond_8_days=national_metrics_by_month.integrated.beyond_8_days,
    )

    national_data = NationalData(
        transfer_count=national_metrics_by_month.transfer_count, integrated=integrated_data
    )

    return NationalDataPlatformData(generated_on=current_datetime, metrics=national_data)
