from dataclasses import dataclass
from datetime import datetime
from typing import List

from dateutil.tz import tzutc

from prmdata.domain.gp2gp.national_metrics import NationalMetrics


@dataclass
class PaperMetrics:
    transfer_count: int
    transfer_percentage: float


@dataclass
class IntegratedMetrics:
    transfer_count: int
    transfer_percentage: float
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


@dataclass
class MonthlyNationalMetrics:
    transfer_count: int
    integrated: IntegratedMetrics
    paper_fallback: PaperMetrics
    year: int
    month: int


@dataclass
class NationalMetricsPresentation:
    generated_on: datetime
    metrics: List[MonthlyNationalMetrics]


def calculate_percentage(portion: int, total: int):
    if total == 0:
        return None
    return round((portion / total) * 100, 2)


def construct_national_metrics(
    national_metrics: NationalMetrics,
    year: int,
    month: int,
) -> NationalMetricsPresentation:
    current_datetime = datetime.now(tzutc())

    paper_fallback_count = (
        national_metrics.transfers_initiated_count
        - national_metrics.integrated.within_3_days
        - national_metrics.integrated.within_8_days
    )

    return NationalMetricsPresentation(
        generated_on=current_datetime,
        metrics=[
            MonthlyNationalMetrics(
                transfer_count=national_metrics.transfers_initiated_count,
                integrated=IntegratedMetrics(
                    transfer_percentage=calculate_percentage(
                        portion=national_metrics.integrated.transfer_count,
                        total=national_metrics.transfers_initiated_count,
                    ),
                    transfer_count=national_metrics.integrated.transfer_count,
                    within_3_days=national_metrics.integrated.within_3_days,
                    within_8_days=national_metrics.integrated.within_8_days,
                    beyond_8_days=national_metrics.integrated.beyond_8_days,
                ),
                paper_fallback=PaperMetrics(
                    transfer_count=paper_fallback_count,
                    transfer_percentage=calculate_percentage(
                        portion=paper_fallback_count,
                        total=national_metrics.transfers_initiated_count,
                    ),
                ),
                year=year,
                month=month,
            )
        ],
    )
