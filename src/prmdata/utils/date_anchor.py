from datetime import datetime

from dateutil.relativedelta import relativedelta


class DateAnchor:
    def __init__(self, now: datetime):
        self.current_month = datetime(now.year, now.month, 1)

    @property
    def previous_month(self):
        return self.current_month - relativedelta(months=1)
