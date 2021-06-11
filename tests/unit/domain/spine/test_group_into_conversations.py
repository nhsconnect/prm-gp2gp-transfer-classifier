from datetime import datetime, timedelta

import pytest

from prmdata.domain.spine.conversation import Conversation, group_into_conversations
from tests.builders.spine import build_message


def test_produces_correct_conversations():
    message_one = build_message(conversation_id="abc")
    message_two = build_message(conversation_id="xyz")
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one]), Conversation("xyz", [message_two])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_produces_correct_messages_within_conversations():
    message_one = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=5))
    message_two = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=6))
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one, message_two])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_sorts_messages_within_conversations():
    message_one = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=6))
    message_two = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=5))
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_two, message_one])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


@pytest.mark.parametrize(
    ["cutoff_interval", "expected_message_ids"],
    [
        (timedelta(days=0, hours=0), ["1"]),
        (timedelta(days=0, hours=12), ["1"]),
        (timedelta(days=1, hours=12), ["1", "2"]),
        (timedelta(days=2, hours=12), ["1", "2", "3"]),
    ],
)
def test_rejects_messages_after_cutoff(cutoff_interval, expected_message_ids):
    messages = [
        build_message(conversation_id="a", guid="1", time=datetime(year=2020, month=6, day=6)),
        build_message(conversation_id="a", guid="2", time=datetime(year=2020, month=6, day=7)),
        build_message(conversation_id="a", guid="3", time=datetime(year=2020, month=6, day=8)),
    ]

    conversations = group_into_conversations(messages, cutoff=cutoff_interval)
    actual_message_ids = [m.guid for m in next(conversations).messages]

    assert actual_message_ids == expected_message_ids


def test_has_no_cutoff_by_default():
    message_one = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=6))
    message_two = build_message(conversation_id="abc", time=datetime(year=2060, month=6, day=6))
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one, message_two])]

    actual = group_into_conversations(messages, cutoff=None)

    assert list(actual) == expected
