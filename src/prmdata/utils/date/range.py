from datetime import datetime


class DateTimeRange:
    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end = end

    def contains(self, time: datetime):
        return self.start <= time < self.end
