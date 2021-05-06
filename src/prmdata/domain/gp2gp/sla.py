from collections import defaultdict
from datetime import timedelta
from enum import Enum, auto

THREE_DAYS_IN_SECONDS = 259200
EIGHT_DAYS_IN_SECONDS = 691200


class SlaBand(Enum):
    WITHIN_3_DAYS = auto()
    WITHIN_8_DAYS = auto()
    BEYOND_8_DAYS = auto()


def assign_to_sla_band(sla_duration: timedelta) -> SlaBand:
    sla_duration_in_seconds = sla_duration.total_seconds()
    if sla_duration_in_seconds <= THREE_DAYS_IN_SECONDS:
        return SlaBand.WITHIN_3_DAYS
    elif sla_duration_in_seconds <= EIGHT_DAYS_IN_SECONDS:
        return SlaBand.WITHIN_8_DAYS
    else:
        return SlaBand.BEYOND_8_DAYS


class SlaCounter:
    def __init__(self):
        self._counts = defaultdict(int)

    def increment(self, duration: timedelta):
        sla_band = assign_to_sla_band(duration)
        self._counts[sla_band] += 1

    def total(self) -> int:
        return sum(self._counts.values())

    def within_3_days(self) -> int:
        return self._counts[SlaBand.WITHIN_3_DAYS]

    def within_8_days(self):
        return self._counts[SlaBand.WITHIN_8_DAYS]

    def beyond_8_days(self):
        return self._counts[SlaBand.BEYOND_8_DAYS]
