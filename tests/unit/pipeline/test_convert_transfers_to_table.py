from datetime import datetime, timedelta

import pyarrow as pa

from prmdata.domain.gp2gp.transfer_outcome import TransferOutcome, TransferStatus
from prmdata.pipeline.arrow import convert_transfers_to_table
from tests.builders.gp2gp import build_practice, build_transfer


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
    transfer = build_transfer(requesting_practice=build_practice(asid="003212345678"))

    expected_asid_column = {"requesting_practice_asid": ["003212345678"]}

    table = convert_transfers_to_table([transfer])
    actual_asid_column = table.select(["requesting_practice_asid"]).to_pydict()

    assert actual_asid_column == expected_asid_column


def test_sending_practice_asid_is_converted_to_column():
    transfer = build_transfer(sending_practice=build_practice(asid="001112345678"))

    expected_asid_column = {"sending_practice_asid": ["001112345678"]}

    table = convert_transfers_to_table([transfer])
    actual_asid_column = table.select(["sending_practice_asid"]).to_pydict()

    assert actual_asid_column == expected_asid_column


def test_requesting_practice_ods_is_converted_to_column():
    transfer = build_transfer(requesting_practice=build_practice(ods_code="A12"))

    expected_ods_column = {"requesting_practice_ods_code": ["A12"]}

    table = convert_transfers_to_table([transfer])
    actual_ods_column = table.select(["requesting_practice_ods_code"]).to_pydict()

    assert actual_ods_column == expected_ods_column


def test_requesting_practice_name_is_converted_to_column():
    transfer = build_transfer(
        requesting_practice=build_practice(
            name="Test Requesting GP Practice",
        )
    )

    expected_name_column = {"requesting_practice_name": ["Test Requesting GP Practice"]}

    table = convert_transfers_to_table([transfer])
    actual_name_column = table.select(["requesting_practice_name"]).to_pydict()

    assert actual_name_column == expected_name_column


def test_requesting_practice_sicbl_ods_is_converted_to_column():
    transfer = build_transfer(requesting_practice=build_practice(sicbl_ods_code="14A"))

    expected_ods_column = {"requesting_practice_sicbl_ods_code": ["14A"]}

    table = convert_transfers_to_table([transfer])
    actual_ods_column = table.select(["requesting_practice_sicbl_ods_code"]).to_pydict()

    assert actual_ods_column == expected_ods_column


def test_requesting_practice_sicbl_name_is_converted_to_column():
    transfer = build_transfer(requesting_practice=build_practice(sicbl_name="Testing SICBL Name"))

    expected_sicbl_name_column = {"requesting_practice_sicbl_name": ["Testing SICBL Name"]}

    table = convert_transfers_to_table([transfer])
    actual_ods_column = table.select(["requesting_practice_sicbl_name"]).to_pydict()

    assert actual_ods_column == expected_sicbl_name_column


def test_sending_practice_ods_is_converted_to_column():
    transfer = build_transfer(sending_practice=build_practice(ods_code="A12"))

    expected_ods_column = {"sending_practice_ods_code": ["A12"]}

    table = convert_transfers_to_table([transfer])
    actual_ods_column = table.select(["sending_practice_ods_code"]).to_pydict()

    assert actual_ods_column == expected_ods_column


def test_sending_practice_sicbl_ods_is_converted_to_column():
    transfer = build_transfer(sending_practice=build_practice(sicbl_ods_code="10A"))

    expected_ods_column = {"sending_practice_sicbl_ods_code": ["10A"]}

    table = convert_transfers_to_table([transfer])
    actual_ods_column = table.select(["sending_practice_sicbl_ods_code"]).to_pydict()

    assert actual_ods_column == expected_ods_column


def test_sending_practice_sicbl_name_is_converted_to_column():
    transfer = build_transfer(sending_practice=build_practice(sicbl_name="Testing SICBL Name"))

    expected_sicbl_name_column = {"sending_practice_sicbl_name": ["Testing SICBL Name"]}

    table = convert_transfers_to_table([transfer])
    actual_ods_column = table.select(["sending_practice_sicbl_name"]).to_pydict()

    assert actual_ods_column == expected_sicbl_name_column


def test_sender_error_codes_are_converted_to_column():
    transfer = build_transfer(sender_error_codes=[10])

    expected_error_code_column = {"sender_error_codes": [[10]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["sender_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_sender_error_codes_are_converted_to_column_when_missing():
    transfer = build_transfer(sender_error_codes=[None])

    expected_error_code_column = {"sender_error_codes": [[None]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["sender_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_final_error_codes_are_converted_to_column():
    transfer = build_transfer(final_error_codes=[5])

    expected_error_code_column = {"final_error_codes": [[5]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["final_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_final_error_codes_are_converted_to_column_when_missing():
    transfer = build_transfer(final_error_codes=[])

    expected_error_code_column: dict = {"final_error_codes": [[]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["final_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_intermediate_error_codes_are_converted_to_column():
    transfer = build_transfer(intermediate_error_codes=[6])

    expected_error_code_column = {"intermediate_error_codes": [[6]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["intermediate_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_intermediate_error_codes_are_converted_to_column_when_empty():
    transfer = build_transfer(intermediate_error_codes=[])

    expected_error_code_column: dict = {"intermediate_error_codes": [[]]}

    table = convert_transfers_to_table([transfer])
    actual_error_code_column = table.select(["intermediate_error_codes"]).to_pydict()

    assert actual_error_code_column == expected_error_code_column


def test_status_is_converted_to_column():
    integrated_transfer_outcome = TransferOutcome(
        status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None
    )
    transfer = build_transfer(transfer_outcome=integrated_transfer_outcome)

    expected_status_column = {"status": ["Integrated on time"]}

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
            ("requesting_practice_ods_code", pa.string()),
            ("requesting_practice_name", pa.string()),
            ("requesting_practice_sicbl_ods_code", pa.string()),
            ("requesting_practice_sicbl_name", pa.string()),
            ("sending_practice_asid", pa.string()),
            ("sending_practice_ods_code", pa.string()),
            ("sending_practice_name", pa.string()),
            ("sending_practice_sicbl_ods_code", pa.string()),
            ("sending_practice_sicbl_name", pa.string()),
            ("requesting_supplier", pa.string()),
            ("sending_supplier", pa.string()),
            ("sender_error_codes", pa.list_(pa.int64())),
            ("final_error_codes", pa.list_(pa.int64())),
            ("intermediate_error_codes", pa.list_(pa.int64())),
            ("status", pa.string()),
            ("failure_reason", pa.string()),
            ("date_requested", pa.timestamp("us")),
            ("date_completed", pa.timestamp("us")),
            ("last_sender_message_timestamp", pa.timestamp("us")),
        ]
    )

    table = convert_transfers_to_table(transfers)
    actual_schema = table.schema

    assert actual_schema == expected_schema
