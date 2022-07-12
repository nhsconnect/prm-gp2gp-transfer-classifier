from datetime import datetime, timedelta

from prmdata.domain.gp2gp.transfer import Practice, Transfer
from prmdata.domain.gp2gp.transfer_outcome import (
    TransferFailureReason,
    TransferOutcome,
    TransferStatus,
)
from prmdata.pipeline.io import TransferClassifierIO
from prmdata.utils.input_output.s3 import S3DataManager
from tests.builders.common import a_string
from tests.builders.gp2gp import build_transfer
from tests.builders.s3 import MockS3

_SOME_METADATA: dict[str, str] = {}


def test_write_transfers_correctly_writes_all_fields():
    mock_s3 = MockS3()
    s3_data_manager = S3DataManager(mock_s3)
    io = TransferClassifierIO(s3_data_manager)

    transfer = Transfer(
        conversation_id="1234",
        sla_duration=timedelta(days=1),
        requesting_practice=Practice(
            asid="123",
            supplier="Supplier A",
            ods_code="A12",
            name="Test Requesting GP Practice Name",
            sicbl_ods_code="11B",
            sicbl_name="Test Requesting GP SICBL Name",
        ),
        sending_practice=Practice(
            asid="456",
            supplier="Supplier B",
            ods_code="B12",
            sicbl_ods_code="10A",
            name="Test Sending GP Practice Name",
            sicbl_name="Test Sending GP SICBL Name",
        ),
        sender_error_codes=[1, None],
        final_error_codes=[None, 32],
        intermediate_error_codes=[],
        outcome=TransferOutcome(
            status=TransferStatus.PROCESS_FAILURE, failure_reason=TransferFailureReason.FINAL_ERROR
        ),
        date_requested=datetime(year=2021, month=3, day=5),
        date_completed=None,
        last_sender_message_timestamp=None,
    )

    io.write_transfers(
        transfers=[transfer], s3_uri="s3://a_bucket/some_data.parquet", metadata=_SOME_METADATA
    )

    expected_table = {
        "conversation_id": ["1234"],
        "sla_duration": [86400],
        "requesting_practice_asid": ["123"],
        "requesting_practice_ods_code": ["A12"],
        "requesting_practice_name": ["Test Requesting GP Practice Name"],
        "requesting_practice_sicbl_ods_code": ["11B"],
        "requesting_practice_sicbl_name": ["Test Requesting GP SICBL Name"],
        "sending_practice_asid": ["456"],
        "sending_practice_ods_code": ["B12"],
        "sending_practice_name": ["Test Sending GP Practice Name"],
        "sending_practice_sicbl_ods_code": ["10A"],
        "sending_practice_sicbl_name": ["Test Sending GP SICBL Name"],
        "requesting_supplier": ["Supplier A"],
        "sending_supplier": ["Supplier B"],
        "sender_error_codes": [[1, None]],
        "final_error_codes": [[None, 32]],
        "intermediate_error_codes": [[]],
        "status": ["Process failure"],
        "failure_reason": ["Final error"],
        "date_requested": [datetime(year=2021, month=3, day=5)],
        "date_completed": [None],
        "last_sender_message_timestamp": [None],
    }

    actual_table = mock_s3.object("a_bucket", "some_data.parquet").read_parquet().to_pydict()

    assert actual_table == expected_table


def test_write_transfers_correctly_writes_multiple_rows():
    mock_s3 = MockS3()
    s3_data_manager = S3DataManager(mock_s3)
    io = TransferClassifierIO(s3_data_manager)

    transfers = [
        build_transfer(conversation_id="a"),
        build_transfer(conversation_id="b"),
        build_transfer(conversation_id="c"),
    ]

    io.write_transfers(
        transfers=transfers, s3_uri="s3://a_bucket/multi_row.parquet", metadata=_SOME_METADATA
    )

    expected_conversation_ids = ["a", "b", "c"]

    actual_conversation_ids = (
        mock_s3.object("a_bucket", "multi_row.parquet")
        .read_parquet()
        .to_pydict()
        .get("conversation_id")
    )

    assert actual_conversation_ids == expected_conversation_ids


def test_write_transfers_writes_metadata():
    mock_s3 = MockS3()
    s3_data_manager = S3DataManager(mock_s3)

    metadata = {a_string(): a_string()}

    io = TransferClassifierIO(s3_data_manager)

    io.write_transfers(
        transfers=[build_transfer()], s3_uri="s3://a_bucket/some_data.parquet", metadata=metadata
    )

    actual_meta_data = mock_s3.object("a_bucket", "some_data.parquet").get_metadata()

    assert actual_meta_data == metadata
