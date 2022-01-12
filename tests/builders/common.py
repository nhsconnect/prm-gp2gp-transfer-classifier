import random
import string
import sys
from datetime import datetime, timedelta

from dateutil.tz import UTC


def a_string(length=10, characters=string.ascii_letters + string.digits):
    return "".join(random.choice(characters) for _ in range(length))


def an_integer(a=None, b=None):
    return random.randint(a if a else 0, b if b else sys.maxsize)


def a_datetime(**kwargs):
    return datetime(
        year=kwargs.get("year", an_integer(1, 9999)),
        month=kwargs.get("month", an_integer(1, 12)),
        day=kwargs.get("day", an_integer(1, 28)),
        hour=kwargs.get("hour", an_integer(0, 23)),
        minute=kwargs.get("minute", an_integer(0, 59)),
        second=kwargs.get("second", an_integer(0, 59)),
        tzinfo=UTC,
    )


def a_duration(max_length=1000000):
    return timedelta(seconds=an_integer(10, max_length))
