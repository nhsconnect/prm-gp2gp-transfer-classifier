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
class FailedMetrics:
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
    failed: FailedMetrics
    paper_fallback: PaperFallbackMetrics
    year: int
    month: int


@dataclass
class NationalMetricsPresentation:
    generated_on: datetime
    metrics: List[MonthlyNationalMetrics]


def _construct_integrated_metrics(national_metrics: NationalMetrics) -> IntegratedMetrics:
    return IntegratedMetrics(
        transfer_percentage=calculate_percentage(
            portion=national_metrics.integrated.transfer_count,
            total=national_metrics.initiated_transfer_count,
            num_digits=2,
        ),
        transfer_count=national_metrics.integrated.transfer_count,
        within_3_days=national_metrics.integrated.within_3_days,
        within_8_days=national_metrics.integrated.within_8_days,
        beyond_8_days=national_metrics.integrated.beyond_8_days,
    )


def _construct_failed_metrics(national_metrics: NationalMetrics) -> FailedMetrics:
    return FailedMetrics(
        transfer_count=national_metrics.failed_transfer_count,
        transfer_percentage=calculate_percentage(
            portion=national_metrics.failed_transfer_count,
            total=national_metrics.initiated_transfer_count,
            num_digits=2,
        ),
    )


def _construct_paper_fallback_metrics(national_metrics: NationalMetrics) -> PaperFallbackMetrics:
    paper_fallback_count = national_metrics.calculate_paper_fallback()

    return PaperFallbackMetrics(
        transfer_count=paper_fallback_count,
        transfer_percentage=calculate_percentage(
            portion=paper_fallback_count,
            total=national_metrics.initiated_transfer_count,
            num_digits=2,
        ),
    )


def construct_national_metrics(
    national_metrics: NationalMetrics,
    year: int,
    month: int,
) -> NationalMetricsPresentation:
    current_datetime = datetime.now(tzutc())

    return NationalMetricsPresentation(
        generated_on=current_datetime,
        metrics=[
            MonthlyNationalMetrics(
                transfer_count=national_metrics.initiated_transfer_count,
                integrated=_construct_integrated_metrics(national_metrics),
                failed=_construct_failed_metrics(national_metrics),
                paper_fallback=_construct_paper_fallback_metrics(national_metrics),
                year=year,
                month=month,
            )
        ],
    )
