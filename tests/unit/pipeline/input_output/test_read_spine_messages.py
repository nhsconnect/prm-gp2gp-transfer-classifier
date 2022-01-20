from datetime import datetime

from dateutil.tz import tzutc

from prmdata.domain.spine.message import Message
from prmdata.pipeline.io import TransferClassifierIO
from prmdata.utils.input_output.s3 import S3DataManager
from tests.builders.common import a_string
from tests.builders.file import build_gzip_csv
from tests.builders.s3 import MockS3, MockS3Object
from tests.builders.spine import build_spine_item

_SPINE_CSV_COLUMNS = [
    "_time",
    "conversationID",
    "GUID",
    "interactionID",
    "messageSender",
    "messageRecipient",
    "messageRef",
    "fromSystem",
    "toSystem",
    "jdiEvent",
]


def _spine_csv_gz(rows):
    return build_gzip_csv(
        header=_SPINE_CSV_COLUMNS,
        rows=[[row.get(field, a_string()) for field in _SPINE_CSV_COLUMNS] for row in rows],
    )


def test_read_spine_messages_reads_single_message_correctly():
    csv_row = build_spine_item(
        time="2019-12-31T23:37:55.334+0000",
        conversation_id="abc",
        guid="message_a",
        interaction_id="an_interaction_id",
        message_sender="sender_x",
        message_recipient="recipient_y",
        message_ref="NotProvided",
        jdi_event="NONE",
        raw="",
        from_system="SupplierA",
        to_system="Unknown",
    )

    mock_s3_conn = MockS3(
        objects=[
            MockS3Object(
                bucket="test_bucket", key="data/1.csv.gz", contents=_spine_csv_gz([csv_row])
            )
        ]
    )

    io = TransferClassifierIO(s3_data_manager=S3DataManager(mock_s3_conn))

    expected_spine_message = Message(
        time=datetime(2019, 12, 31, 23, 37, 55, 334000, tzutc()),
        conversation_id="abc",
        guid="message_a",
        interaction_id="an_interaction_id",
        from_party_asid="sender_x",
        to_party_asid="recipient_y",
        message_ref=None,
        error_code=None,
        from_system="SupplierA",
        to_system="Unknown",
    )

    actual = io.read_spine_messages(["s3://test_bucket/data/1.csv.gz"])

    assert list(actual) == [expected_spine_message]


def test_read_spine_messages_reads_multiple_messages():
    csv_rows = [build_spine_item(guid=f"guid{i}") for i in range(10)]

    mock_s3_conn = MockS3(
        objects=[
            MockS3Object(
                bucket="test_bucket",
                key="data/1.csv.gz",
                contents=_spine_csv_gz(csv_rows[:4]),
            ),
            MockS3Object(
                bucket="test_bucket",
                key="data/2.csv.gz",
                contents=_spine_csv_gz(csv_rows[4:]),
            ),
        ]
    )

    io = TransferClassifierIO(s3_data_manager=S3DataManager(mock_s3_conn))

    expected_guids = [f"guid{i}" for i in range(10)]

    actual_messages = io.read_spine_messages(
        ["s3://test_bucket/data/1.csv.gz", "s3://test_bucket/data/2.csv.gz"]
    )

    actual_guids = [message.guid for message in actual_messages]

    assert actual_guids == expected_guids
