from datetime import datetime


class DateAnchor:
    def __init__(self, moment: datetime):
        self._moment = moment

    @property
    def current_year(self):
        return self._moment.year

    @property
    def current_month(self):
        return self._moment.month
