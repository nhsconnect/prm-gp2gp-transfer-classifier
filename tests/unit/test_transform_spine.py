from datetime import datetime
from dateutil.tz import tzutc

from gp2gp.models.spine import Conversation, ParsedConversation, Message
from gp2gp.transformers.spine import group_into_conversations, parse_conversation
from tests.builders.spine import build_message


def test_group_into_conversations_produces_correct_conversations():
    message_one = build_message(conversation_id="abc")
    message_two = build_message(conversation_id="xyz")
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one]), Conversation("xyz", [message_two])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_group_into_conversations_produces_correct_messages_within_conversations():
    message_one = build_message(conversation_id="abc")
    message_two = build_message(conversation_id="abc")
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_one, message_two])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_group_into_conversations_sorts_messages_within_conversations():
    message_one = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=6))
    message_two = build_message(conversation_id="abc", time=datetime(year=2020, month=6, day=5))
    messages = [message_one, message_two]

    expected = [Conversation("abc", [message_two, message_one])]

    actual = group_into_conversations(messages)

    assert list(actual) == expected


def test_parse_conversation_parses_a_complete_conversation():
    request_started_message = Message(
        time=datetime(2019, 12, 31, 18, 44, 46, 647000, tzinfo=tzutc()),
        conversation_id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        guid="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN010000UK05",
        from_party_ods="G85055",
        to_party_ods="G85674",
        message_ref="NotProvided",
        error_code=None,
    )
    request_completed_message = Message(
        time=datetime(2019, 12, 31, 18, 44, 58, 53000, tzinfo=tzutc()),
        conversation_id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        guid="54F949C0-DC7F-4EBC-8AE2-72BF2D0AF4EE",
        interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN030000UK06",
        from_party_ods="G85674",
        to_party_ods="G85055",
        message_ref="NotProvided",
        error_code=None,
    )
    request_started_ack_message = Message(
        time=datetime(2019, 12, 31, 18, 44, 59, 381000, tzinfo=tzutc()),
        conversation_id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        guid="A5A34B66-9481-4F92-AB11-6494328B3C38",
        interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
        from_party_ods="G85674",
        to_party_ods="G85055",
        message_ref="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        error_code=None,
    )
    request_completed_ack_message = Message(
        time=datetime(2019, 12, 31, 19, 36, 52, 995000, tzinfo=tzutc()),
        conversation_id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        guid="209520E3-D6D5-4343-BA8F-AF857A8F9652",
        interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
        from_party_ods="G85055",
        to_party_ods="",
        message_ref="54F949C0-DC7F-4EBC-8AE2-72BF2D0AF4EE",
        error_code=None,
    )

    messages = [
        request_started_message,
        request_completed_message,
        request_started_ack_message,
        request_completed_ack_message,
    ]

    conversation = Conversation("F8DAFCAA-5012-427B-BDB4-354256A4874B", messages)

    expected = ParsedConversation(
        id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        request_started=request_started_message,
        request_completed=request_completed_message,
        request_completed_ack=request_completed_ack_message,
    )
    actual = parse_conversation(conversation)

    assert actual == expected


def test_parse_conversation_returns_none_when_start_omitted():
    request_completed_ack_message = Message(
        time=datetime(2019, 12, 31, 19, 36, 52, 995000, tzinfo=tzutc()),
        conversation_id="F8DAFCAA-5012-427B-BDB4-354256A4874B",
        guid="209520E3-D6D5-4343-BA8F-AF857A8F9652",
        interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
        from_party_ods="G85055",
        to_party_ods="",
        message_ref="54F949C0-DC7F-4EBC-8AE2-72BF2D0AF4EE",
        error_code=None,
    )

    messages = [
        request_completed_ack_message,
    ]

    conversation = Conversation("F8DAFCAA-5012-427B-BDB4-354256A4874B", messages)

    expected = None

    actual = parse_conversation(conversation)

    assert actual == expected
