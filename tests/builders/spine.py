import string
import sys
import random
from datetime import datetime

from gp2gp.models.spine import Message


def a_string(length=10, characters=string.ascii_letters + string.digits):
    return "".join(random.choice(characters) for _ in range(length))


def an_integer(a=None, b=None):
    return random.randint(a if a else 0, b if b else sys.maxsize)


def a_datetime():
    return datetime(
        year=an_integer(1, 9999),
        month=an_integer(1, 12),
        day=an_integer(1, 28),
        hour=an_integer(0, 23),
        minute=an_integer(0, 59),
        second=an_integer(0, 59),
    )


def build_message(**kwargs):
    return Message(
        time=kwargs.get("time", a_datetime()),
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        guid=a_string(36),
        interaction_id=a_string(17),
        from_party_ods=a_string(6),
        to_party_ods=a_string(6),
        message_ref=a_string(36),
        error_code=None,
    )
