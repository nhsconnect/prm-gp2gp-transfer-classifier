from datetime import datetime, timedelta

from gp2gp.transformers.gp2gp import build_transfer
from tests.builders.spine import build_parsed_conversation, build_message


def test_build_transfer_extracts_conversation_id():
    conversation = build_parsed_conversation(id="1234")

    transfer = build_transfer(conversation)

    expected = "1234"
    actual = transfer.conversation_id
    assert actual == expected


def test_build_transfer_produces_sla_of_successful_conversation():
    conversation = build_parsed_conversation(
        request_completed=build_message(
            time=datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
        ),
        request_completed_ack=build_message(
            time=datetime(year=2020, month=6, day=1, hour=13, minute=52, second=0), error_code=None
        ),
    )

    transfer = build_transfer(conversation)

    expected = timedelta(hours=1, minutes=10)
    actual = transfer.sla_duration
    assert actual == expected


def test_build_transfer_extracts_requesting_practice_ods():
    conversation = build_parsed_conversation(
        id="1234", request_started=build_message(from_party_ods="A12345")
    )

    transfer = build_transfer(conversation)

    expected = "A12345"
    actual = transfer.requesting_practice_ods
    assert actual == expected
