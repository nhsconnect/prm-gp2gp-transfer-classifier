from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List

from dateutil.tz import tzutc

from prmdata.domain.gp2gp.practice_metrics import PracticeMetrics
from prmdata.utils.calculate_percentage import calculate_percentage


@dataclass
class IntegratedPracticeMetrics:
    transfer_count: int
    within_3_days_percentage: float
    within_8_days_percentage: float
    beyond_8_days_percentage: float


@dataclass
class RequesterMetrics:
    integrated: IntegratedPracticeMetrics


@dataclass
class MonthlyMetrics:
    year: int
    month: int
    requester: RequesterMetrics


@dataclass
class PracticeSummary:
    ods_code: str
    name: str
    metrics: List[MonthlyMetrics]


@dataclass
class PracticeMetricsPresentation:
    generated_on: datetime
    practices: List[PracticeSummary]


def construct_practice_metrics(
    sla_metrics: Iterable[PracticeMetrics], year: int, month: int
) -> PracticeMetricsPresentation:
    return PracticeMetricsPresentation(
        generated_on=datetime.now(tzutc()),
        practices=[
            PracticeSummary(
                ods_code=practice.ods_code,
                name=practice.name,
                metrics=[
                    MonthlyMetrics(
                        year=year,
                        month=month,
                        requester=RequesterMetrics(
                            integrated=IntegratedPracticeMetrics(
                                transfer_count=practice.integrated.transfer_count,
                                within_3_days_percentage=calculate_percentage(
                                    portion=practice.integrated.within_3_days,
                                    total=practice.integrated.transfer_count,
                                    num_digits=1,
                                ),
                                within_8_days_percentage=calculate_percentage(
                                    portion=practice.integrated.within_8_days,
                                    total=practice.integrated.transfer_count,
                                    num_digits=1,
                                ),
                                beyond_8_days_percentage=calculate_percentage(
                                    portion=practice.integrated.beyond_8_days,
                                    total=practice.integrated.transfer_count,
                                    num_digits=1,
                                ),
                            ),
                        ),
                    )
                ],
            )
            for practice in sla_metrics
        ],
    )
