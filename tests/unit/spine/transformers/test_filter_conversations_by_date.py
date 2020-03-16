from datetime import datetime

from gp2gp.date.range import DateTimeRange
from gp2gp.spine.transformers import filter_conversations_by_request_started_time
from tests.builders.spine import build_parsed_conversation, build_message


def test_filter_conversations_by_request_started_time_keeps_conversation_within_range():
    date_range = DateTimeRange(
        start=datetime(year=2020, month=6, day=1), end=datetime(year=2020, month=7, day=1)
    )

    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=6, day=6))
        )
    ]

    expected = parsed_conversations

    actual = filter_conversations_by_request_started_time(parsed_conversations, date_range)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversation_before_range():
    date_range = DateTimeRange(
        start=datetime(year=2020, month=6, day=1), end=datetime(year=2020, month=7, day=1)
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=5, day=28))
        )
    ]

    expected = []

    actual = filter_conversations_by_request_started_time(parsed_conversations, date_range)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversation_after_range():
    date_range = DateTimeRange(
        start=datetime(year=2020, month=6, day=1), end=datetime(year=2020, month=7, day=1)
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=7, day=28))
        )
    ]

    expected = []

    actual = filter_conversations_by_request_started_time(parsed_conversations, date_range)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversations_outside_of_range():
    date_range = DateTimeRange(
        start=datetime(year=2020, month=6, day=1), end=datetime(year=2020, month=7, day=1)
    )

    conversation_within_range = build_parsed_conversation(
        request_started=build_message(time=datetime(year=2020, month=6, day=15))
    )
    conversation_before_range = build_parsed_conversation(
        request_started=build_message(time=datetime(year=2020, month=5, day=28))
    )
    conversation_after_range = build_parsed_conversation(
        request_started=build_message(time=datetime(year=2020, month=7, day=28))
    )

    parsed_conversations = [
        conversation_before_range,
        conversation_within_range,
        conversation_after_range,
    ]

    expected = [conversation_within_range]

    actual = filter_conversations_by_request_started_time(parsed_conversations, date_range)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_accepts_conversation_on_range_start():
    date_range = DateTimeRange(
        start=datetime(year=2020, month=6, day=1), end=datetime(year=2020, month=7, day=1)
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=6, day=1))
        )
    ]

    expected = parsed_conversations

    actual = filter_conversations_by_request_started_time(parsed_conversations, date_range)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversation_on_range_end():
    date_range = DateTimeRange(
        start=datetime(year=2020, month=6, day=1), end=datetime(year=2020, month=7, day=1)
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=7, day=1))
        )
    ]

    expected = []

    actual = filter_conversations_by_request_started_time(parsed_conversations, date_range)

    assert list(actual) == expected
