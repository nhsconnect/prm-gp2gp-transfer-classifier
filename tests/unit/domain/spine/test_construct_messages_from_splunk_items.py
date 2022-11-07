from datetime import datetime

import pytest
from dateutil.tz import tzutc

from prmdata.domain.spine.message import (
    FailedToConstructMessagesFromSplunkItemsError,
    Message,
    construct_messages_from_splunk_items,
)
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
            from_system="SupplierA",
            to_system="Unknown",
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
            from_system="SupplierB",
            to_system="SupplierC",
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
            from_system="SupplierA",
            to_system="Unknown",
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
            from_system="SupplierB",
            to_system="SupplierC",
        ),
    ]

    actual = construct_messages_from_splunk_items(items)

    assert list(actual) == expected


def test_returns_correct_message_when_from_system_and_to_system_is_not_present():
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
            from_system=None,
            to_system=None,
        )
    ]

    actual = construct_messages_from_splunk_items(items)

    assert list(actual) == expected


@pytest.mark.parametrize(
    "time_input",
    ["2019-07-01T09:10:00.334+0000", "2019-07-01 09:10:00.334 UTC", "2019-07-01 10:10:00.334 BST"],
)
def test_returns_appropriate_time_given_time_with_british_timezones(time_input):
    items = [
        build_spine_item(
            time=time_input,
            conversation_id="convo_abc",
            guid="message_a",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            message_sender="123456789012",
            message_recipient="121212121212",
            message_ref="NotProvided",
            jdi_event="NONE",
            raw="",
        )
    ]
    expected_time = datetime(2019, 7, 1, 9, 10, 0, 334000, tzutc())

    messages = construct_messages_from_splunk_items(items)
    actual_time = next(messages).time

    assert actual_time == expected_time


def test_handles_empty_fields():
    items = [
        build_spine_item(
            time="2019-12-31T23:37:55.334+0000",
            conversation_id="",
            guid="",
            interaction_id="",
            message_sender="",
            message_recipient="",
            message_ref="",
            jdi_event="",
            raw="",
        ),
        build_spine_item(
            time="2019-12-31T23:37:55.334+0000",
            conversation_id=None,
            guid=None,
            interaction_id=None,
            message_sender=None,
            message_recipient=None,
            message_ref=None,
            jdi_event=None,
            raw=None,
        ),
    ]

    expected = [
        Message(
            time=datetime(2019, 12, 31, 23, 37, 55, 334000, tzutc()),
            conversation_id="",
            guid="",
            interaction_id="",
            from_party_asid="",
            to_party_asid="",
            message_ref=None,
            error_code=None,
            from_system=None,
            to_system=None,
        ),
        Message(
            time=datetime(2019, 12, 31, 23, 37, 55, 334000, tzutc()),
            conversation_id=None,
            guid=None,
            interaction_id=None,
            from_party_asid=None,
            to_party_asid=None,
            message_ref=None,
            error_code=None,
            from_system=None,
            to_system=None,
        ),
    ]

    actual = construct_messages_from_splunk_items(items)

    assert list(actual) == expected


def test_handles_invalid_data():
    a_time = "2019-07-01T09:10:00.334+0000"
    a_guid = "a_message_guid"
    items = [
        build_spine_item(
            time=a_time,
            conversation_id="convo_abc",
            guid=a_guid,
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            message_sender="123456789012",
            message_recipient="121212121212",
            message_ref="NotProvided",
            jdi_event="INVALID",
            raw="",
        )
    ]

    with pytest.raises(FailedToConstructMessagesFromSplunkItemsError) as e:
        messages = construct_messages_from_splunk_items(items)
        next(messages)

    assert str(e.value) == str(
        FailedToConstructMessagesFromSplunkItemsError(
            f"Failed to construct_messages_from_splunk_items with message GUID: {a_guid} and time: {a_time}",
            ValueError("invalid literal for int() with base 10: 'INVALID'"),
        )
    )
