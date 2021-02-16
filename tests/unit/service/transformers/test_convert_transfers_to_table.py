from datetime import timedelta

from gp2gp.service.transformers import convert_transfers_to_table
from tests.builders.service import build_transfer


def test_conversation_id_is_converted_to_column():
    transfer = build_transfer(conversation_id="123")

    expected_conversation_column = {"conversation_id": ["123"]}

    table = convert_transfers_to_table([transfer])
    actual_conversation_column = table.select(["conversation_id"]).to_pydict()

    assert actual_conversation_column == expected_conversation_column


def test_sla_duration_is_converted_to_column():
    transfer = build_transfer(sla_duration=timedelta(days=2, hours=1, minutes=3, seconds=6))

    expected_sla_duration_column = {"sla_duration": [176586]}

    table = convert_transfers_to_table([transfer])
    actual_sla_duration_column = table.select(["sla_duration"]).to_pydict()

    assert actual_sla_duration_column == expected_sla_duration_column


def test_sla_duration_is_rounded_to_integer():
    transfer = build_transfer(
        sla_duration=timedelta(days=2, hours=1, minutes=3, seconds=6, milliseconds=1)
    )

    expected_sla_duration_column = {"sla_duration": [176586]}

    table = convert_transfers_to_table([transfer])
    actual_sla_duration_column = table.select(["sla_duration"]).to_pydict()

    assert actual_sla_duration_column == expected_sla_duration_column


def test_sla_duration_is_converted_to_column_when_missing():
    transfer = build_transfer(sla_duration=None)

    expected_sla_duration_column = {"sla_duration": [None]}

    table = convert_transfers_to_table([transfer])
    actual_sla_duration_column = table.select(["sla_duration"]).to_pydict()

    assert actual_sla_duration_column == expected_sla_duration_column
