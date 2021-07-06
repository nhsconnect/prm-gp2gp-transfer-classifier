from datetime import timedelta, datetime

import pyarrow as pa

from prmdata.domain.gp2gp.transfer import (
    TransferStatus,
    convert_transfers_to_table,
    TransferFailureReason,
    TransferOutcome,
)
from tests.builders.gp2gp import build_transfer


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


def test_requesting_practice_asid_is_converted_to_column():
    transfer = build_transfer(requesting_practice_asid="003212345678")

    expected_asid_column = {"requesting_practice_asid": ["003212345678"]}

    table = convert_transfers_to_table([transfer])
    actual_asid_column = table.select(["requesting_practice_asid"]).to_pydict()

    assert actual_asid_column == expected_asid_column


def test_sending_practice_asid_is_converted_to_column():
    transfer = build_transfer(sending_practice_asid="001112345678")

    expected_asid_column = {"sending_practice_asid": ["001112345678"]}

    table = convert_transfers_to_table([transfer])
    actual_asid_column = table.select(["sending_practice_asid"]).to_pydict()

    assert actual_asid_column == expected_asid_column


def test_sender_error_code_is_converted_to_column():
    transfer = build_transfer(sender_error_code=10)

    expected_error_code_column = {"sender_error_code": [10]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["sender_error_code"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_sender_error_code_is_converted_to_column_when_missing():
    transfer = build_transfer(sender_error_code=None)

    expected_error_code_column = {"sender_error_code": [None]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["sender_error_code"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_final_error_code_is_converted_to_column():
    transfer = build_transfer(final_error_codes=[5])

    expected_error_code_column = {"final_error_codes": [[5]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["final_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_final_error_code_is_converted_to_column_when_missing():
    transfer = build_transfer(final_error_codes=[])

    expected_error_code_column: dict = {"final_error_codes": [[]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["final_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_intermediate_error_codes_is_converted_to_column():
    transfer = build_transfer(intermediate_error_codes=[6])

    expected_error_code_column = {"intermediate_error_codes": [[6]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["intermediate_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_intermediate_error_codes_is_converted_to_column_when_empty():
    transfer = build_transfer(intermediate_error_codes=[])

    expected_error_code_column: dict = {"intermediate_error_codes": [[]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["intermediate_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_status_is_converted_to_column():
    integrated_transfer_outcome = TransferOutcome(
        status=TransferStatus.INTEGRATED_ON_TIME, reason=TransferFailureReason.DEFAULT
    )
    transfer = build_transfer(transfer_outcome=integrated_transfer_outcome)

    expected_status_column = {"status": ["INTEGRATED_ON_TIME"]}

    table = convert_transfers_to_table([transfer])
    actual_status_column = table.select(["status"]).to_pydict()

    assert actual_status_column == expected_status_column


def test_date_requested_is_converted_to_column():
    transfer = build_transfer(date_requested=datetime(year=2020, month=7, day=23, hour=5))

    expected_date_column = {"date_requested": [datetime(year=2020, month=7, day=23, hour=5)]}

    table = convert_transfers_to_table([transfer])
    actual_date_column = table.select(["date_requested"]).to_pydict()

    assert actual_date_column == expected_date_column


def test_date_completed_is_converted_to_column():
    transfer = build_transfer(date_completed=datetime(year=2020, month=7, day=28, hour=17))

    expected_date_column = {"date_completed": [datetime(year=2020, month=7, day=28, hour=17)]}

    table = convert_transfers_to_table([transfer])
    actual_date_column = table.select(["date_completed"]).to_pydict()

    assert actual_date_column == expected_date_column


def test_date_completed_is_converted_to_column_when_missing():
    transfer = build_transfer(date_completed=None)

    expected_date_column = {"date_completed": [None]}

    table = convert_transfers_to_table([transfer])
    actual_date_column = table.select(["date_completed"]).to_pydict()

    assert actual_date_column == expected_date_column


def test_converts_multiple_rows_into_table():
    transfers = [
        build_transfer(conversation_id="123", final_error_codes=[1]),
        build_transfer(conversation_id="456", final_error_codes=[2]),
        build_transfer(conversation_id="789", final_error_codes=[3]),
    ]

    expected_columns = {
        "conversation_id": ["123", "456", "789"],
        "final_error_codes": [[1], [2], [3]],
    }

    table = convert_transfers_to_table(transfers)
    actual_columns = table.select(["conversation_id", "final_error_codes"]).to_pydict()

    assert actual_columns == expected_columns


def test_table_has_correct_schema():
    transfers = [build_transfer()]

    expected_schema = pa.schema(
        [
            ("conversation_id", pa.string()),
            ("sla_duration", pa.uint64()),
            ("requesting_practice_asid", pa.string()),
            ("sending_practice_asid", pa.string()),
            ("requesting_supplier", pa.string()),
            ("sending_supplier", pa.string()),
            ("sender_error_code", pa.int64()),
            ("final_error_codes", pa.list_(pa.int64())),
            ("intermediate_error_codes", pa.list_(pa.int64())),
            ("status", pa.string()),
            ("failure_reason", pa.string()),
            ("date_requested", pa.timestamp("us")),
            ("date_completed", pa.timestamp("us")),
        ]
    )

    table = convert_transfers_to_table(transfers)
    actual_schema = table.schema

    assert actual_schema == expected_schema
