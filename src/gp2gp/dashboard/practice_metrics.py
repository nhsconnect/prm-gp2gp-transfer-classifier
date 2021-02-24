from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List

from dateutil.tz import tzutc

from gp2gp.service.practice_metrics import PracticeMetrics


@dataclass
class IntegratedPracticeMetrics:
    transfer_count: int
    within_3_days: int
    within_8_days: int
    beyond_8_days: int


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
class PracticeMetricsData:
    generated_on: datetime
    practices: List[PracticeSummary]


def construct_practice_metrics_data(
    sla_metrics: Iterable[PracticeMetrics], year: int, month: int
) -> PracticeMetricsData:

    return PracticeMetricsData(
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
                                within_3_days=practice.integrated.within_3_days,
                                within_8_days=practice.integrated.within_8_days,
                                beyond_8_days=practice.integrated.beyond_8_days,
                            )
                        ),
                    )
                ],
            )
            for practice in sla_metrics
        ],
    )
