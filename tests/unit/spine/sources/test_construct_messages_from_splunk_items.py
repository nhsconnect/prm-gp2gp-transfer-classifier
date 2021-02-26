from datetime import datetime

from dateutil.tz import tzutc

from gp2gp.domain.spine.models import Message
from gp2gp.domain.spine.sources import construct_messages_from_splunk_items
from tests.builders.spine import build_spine_item


def test_returns_correct_messages_given_two_items():
    items = [
        build_spine_item(
            time="2019-12-31T23:37:55.334+0000",
            conversation_id="convo_abc",
            guid="message_a",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            message_sender="123456789012",
            message_recipient="121212121212",
            message_ref="NotProvided",
            jdi_event="NONE",
            raw="",
        ),
        build_spine_item(
            time="2019-12-31T22:16:02.249+0000",
            conversation_id="convo_xyz",
            guid="message_b",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            message_sender="456456456456",
            message_recipient="343434343434",
            message_ref="convo_xyz",
            jdi_event="23",
            raw="",
        ),
    ]

    expected = [
        Message(
            time=datetime(2019, 12, 31, 23, 37, 55, 334000, tzutc()),
            conversation_id="convo_abc",
            guid="message_a",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid="123456789012",
            to_party_asid="121212121212",
            message_ref=None,
            error_code=None,
        ),
        Message(
            time=datetime(2019, 12, 31, 22, 16, 2, 249000, tzutc()),
            conversation_id="convo_xyz",
            guid="message_b",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid="456456456456",
            to_party_asid="343434343434",
            message_ref="convo_xyz",
            error_code=23,
        ),
    ]

    actual = construct_messages_from_splunk_items(items)

    assert list(actual) == expected
