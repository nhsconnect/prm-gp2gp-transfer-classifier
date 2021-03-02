from dataclasses import dataclass
from datetime import datetime
from typing import List

from dateutil.tz import tzutc

from prmdata.domain.gp2gp.national_metrics import NationalMetrics
from prmdata.utils.calculate_percentage import calculate_percentage


@dataclass
class PaperFallbackMetrics:
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
    paper_fallback: PaperFallbackMetrics
    year: int
    month: int


@dataclass
class NationalMetricsPresentation:
    generated_on: datetime
    metrics: List[MonthlyNationalMetrics]


def construct_national_metrics(
    national_metrics: NationalMetrics,
    year: int,
    month: int,
) -> NationalMetricsPresentation:
    current_datetime = datetime.now(tzutc())

    paper_fallback_count = national_metrics.calculate_paper_fallback()

    return NationalMetricsPresentation(
        generated_on=current_datetime,
        metrics=[
            MonthlyNationalMetrics(
                transfer_count=national_metrics.initiated_transfers_count,
                integrated=IntegratedMetrics(
                    transfer_percentage=calculate_percentage(
                        portion=national_metrics.integrated.transfer_count,
                        total=national_metrics.initiated_transfers_count,
                        num_digits=2,
                    ),
                    transfer_count=national_metrics.integrated.transfer_count,
                    within_3_days=national_metrics.integrated.within_3_days,
                    within_8_days=national_metrics.integrated.within_8_days,
                    beyond_8_days=national_metrics.integrated.beyond_8_days,
                ),
                paper_fallback=PaperFallbackMetrics(
                    transfer_count=paper_fallback_count,
                    transfer_percentage=calculate_percentage(
                        portion=paper_fallback_count,
                        total=national_metrics.initiated_transfers_count,
                        num_digits=2,
                    ),
                ),
                year=year,
                month=month,
            )
        ],
    )
