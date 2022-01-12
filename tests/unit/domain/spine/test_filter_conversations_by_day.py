from datetime import datetime
from typing import List
from unittest.mock import Mock

from dateutil.tz import UTC

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation, filter_conversations_by_day
from tests.builders import test_cases

mock_gp2gp_conversation_observability_probe = Mock()


def test_returns_conversation_given_conversation_on_day_start():
    daily_start_datetime = datetime(year=2020, month=6, day=1, tzinfo=UTC)

    gp2gp_conversations = [
        Gp2gpConversation(
            messages=test_cases.request_made(
                request_sent_date=datetime(year=2020, month=6, day=1, tzinfo=UTC)
            ),
            probe=mock_gp2gp_conversation_observability_probe,
        )
    ]

    expected = gp2gp_conversations

    actual = filter_conversations_by_day(gp2gp_conversations, daily_start_datetime)

    assert list(actual) == expected


def test_returns_conversation_given_conversation_within_day():
    daily_start_datetime = datetime(year=2020, month=6, day=1, tzinfo=UTC)

    gp2gp_conversations = [
        Gp2gpConversation(
            messages=test_cases.request_made(
                request_sent_date=datetime(year=2020, month=6, day=1, hour=10, tzinfo=UTC)
            ),
            probe=mock_gp2gp_conversation_observability_probe,
        )
    ]

    expected = gp2gp_conversations

    actual = filter_conversations_by_day(gp2gp_conversations, daily_start_datetime)

    assert list(actual) == expected


def test_returns_no_conversation_given_conversation_before_day():
    daily_start_datetime = datetime(year=2020, month=6, day=1, tzinfo=UTC)

    gp2gp_conversations = [
        Gp2gpConversation(
            messages=test_cases.request_made(
                request_sent_date=datetime(
                    year=2020, month=5, day=31, hour=23, minute=59, tzinfo=UTC
                )
            ),
            probe=mock_gp2gp_conversation_observability_probe,
        )
    ]

    expected: List[Gp2gpConversation] = []

    actual = filter_conversations_by_day(gp2gp_conversations, daily_start_datetime)

    assert list(actual) == expected


def test_returns_no_conversation_given_conversation_on_day_end():
    daily_start_datetime = datetime(year=2020, month=2, day=28, tzinfo=UTC)

    gp2gp_conversations = [
        Gp2gpConversation(
            messages=test_cases.request_made(
                request_sent_date=datetime(
                    year=2020, month=2, day=29, hour=0, minute=0, second=0, tzinfo=UTC
                )
            ),
            probe=mock_gp2gp_conversation_observability_probe,
        )
    ]

    expected: List[Gp2gpConversation] = []

    actual = filter_conversations_by_day(gp2gp_conversations, daily_start_datetime)

    assert list(actual) == expected


def test_returns_no_conversation_given_conversation_after_day():
    daily_start_datetime = datetime(year=2020, month=6, day=1, tzinfo=UTC)

    gp2gp_conversations = [
        Gp2gpConversation(
            messages=test_cases.request_made(
                request_sent_date=datetime(year=2020, month=6, day=5, tzinfo=UTC)
            ),
            probe=mock_gp2gp_conversation_observability_probe,
        )
    ]

    expected: List[Gp2gpConversation] = []

    actual = filter_conversations_by_day(gp2gp_conversations, daily_start_datetime)

    assert list(actual) == expected


def test_returns_one_conversation_given_conversations_over_span_of_days():
    daily_start_datetime = datetime(year=2020, month=1, day=1, tzinfo=UTC)

    conversation_within_day = Gp2gpConversation(
        messages=test_cases.request_made(
            request_sent_date=datetime(year=2020, month=1, day=1, tzinfo=UTC)
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    conversation_before_day = Gp2gpConversation(
        messages=test_cases.request_made(
            request_sent_date=datetime(year=2019, month=12, day=31, tzinfo=UTC)
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    conversation_after_day = Gp2gpConversation(
        messages=test_cases.request_made(
            request_sent_date=datetime(year=2020, month=1, day=2, tzinfo=UTC)
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )

    gp2gp_conversations = [
        conversation_before_day,
        conversation_within_day,
        conversation_after_day,
    ]

    expected = [conversation_within_day]

    actual = filter_conversations_by_day(gp2gp_conversations, daily_start_datetime)

    assert list(actual) == expected
