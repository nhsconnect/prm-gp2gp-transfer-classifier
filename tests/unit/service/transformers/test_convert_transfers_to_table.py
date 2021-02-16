from gp2gp.service.transformers import convert_transfers_to_table
from tests.builders.service import build_transfer


def test_conversation_id_is_converted_to_column():
    transfer = build_transfer(conversation_id="123")

    expected_conversation_column = {"conversation_id": ["123"]}

    table = convert_transfers_to_table([transfer])
    actual_conversation_column = table.select(["conversation_id"]).to_pydict()

    assert actual_conversation_column == expected_conversation_column
