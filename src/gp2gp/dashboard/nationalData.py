from dataclasses import dataclass
from datetime import datetime
from dateutil.tz import tzutc

from gp2gp.service.nationalMetricsByMonth import NationalMetricsByMonth


@dataclass
class NationalDataPlatformData:
    generated_on: datetime
    metrics: NationalMetricsByMonth


def construct_national_data_platform_data(
    national_metrics_by_month: NationalMetricsByMonth,
) -> NationalDataPlatformData:
    current_datetime = datetime.now(tzutc())

    return NationalDataPlatformData(
        generated_on=current_datetime, metrics=national_metrics_by_month
    )
