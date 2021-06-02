from datetime import datetime


class DateAnchor:
    def __init__(self, moment: datetime):
        self._moment = moment

    @property
    def current_year(self) -> str:
        return str(self._moment.year)

    @property
    def current_month(self) -> str:
        return str(self._moment.month)
