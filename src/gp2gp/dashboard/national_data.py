from dataclasses import dataclass
from datetime import datetime
from typing import List

from dateutil.tz import tzutc

from gp2gp.service.national_metrics_by_month import NationalMetricsByMonth


@dataclass
class DataPlatformPaperMetrics:
    transfer_count: int


@dataclass
class DataPlatformIntegratedMetrics:
    transfer_count: int
    transfer_percentage: float
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


@dataclass
class DataPlatformNationalMetrics:
    transfer_count: int
    integrated: DataPlatformIntegratedMetrics
    paper_fallback: DataPlatformPaperMetrics
    year: int
    month: int


@dataclass
class NationalDataPlatformData:
    generated_on: datetime
    metrics: List[DataPlatformNationalMetrics]


def calculate_percentage(total: int, portion: int):
    if total == 0:
        return None
    return round((portion / total) * 100, 2)


def construct_national_data_platform_data(
    national_metrics_by_month: NationalMetricsByMonth,
) -> NationalDataPlatformData:
    current_datetime = datetime.now(tzutc())

    paper_fallback_count = (
        national_metrics_by_month.transfer_count
        - national_metrics_by_month.integrated.within_3_days
        - national_metrics_by_month.integrated.within_8_days
    )

    return NationalDataPlatformData(
        generated_on=current_datetime,
        metrics=[
            DataPlatformNationalMetrics(
                transfer_count=national_metrics_by_month.transfer_count,
                integrated=DataPlatformIntegratedMetrics(
                    transfer_percentage=calculate_percentage(
                        total=national_metrics_by_month.transfer_count,
                        portion=national_metrics_by_month.integrated.transfer_count,
                    ),
                    transfer_count=national_metrics_by_month.integrated.transfer_count,
                    within_3_days=national_metrics_by_month.integrated.within_3_days,
                    within_8_days=national_metrics_by_month.integrated.within_8_days,
                    beyond_8_days=national_metrics_by_month.integrated.beyond_8_days,
                ),
                paper_fallback=DataPlatformPaperMetrics(transfer_count=paper_fallback_count),
                year=national_metrics_by_month.year,
                month=national_metrics_by_month.month,
            )
        ],
    )
