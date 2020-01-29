from datetime import datetime

from prmdata.spine.transformers import filter_conversations_by_request_started_time
from tests.builders.spine import build_parsed_conversation, build_message


def test_filter_conversations_by_request_started_time_keeps_conversation_within_range():
    from_time = datetime(year=2020, month=6, day=1)
    to_time = datetime(year=2020, month=6, day=30)
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=6, day=6))
        )
    ]

    expected = parsed_conversations

    actual = filter_conversations_by_request_started_time(parsed_conversations, from_time, to_time)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversation_before_range():
    from_time = datetime(year=2020, month=6, day=1)
    to_time = datetime(year=2020, month=6, day=30)
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=5, day=28))
        )
    ]

    expected = []

    actual = filter_conversations_by_request_started_time(parsed_conversations, from_time, to_time)

    assert list(actual) == expected
