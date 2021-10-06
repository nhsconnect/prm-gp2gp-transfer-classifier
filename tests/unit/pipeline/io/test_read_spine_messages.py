from collections import OrderedDict

from prmdata.pipeline.io import TransferClassifierIO
from prmdata.utils.io.s3 import S3DataManager
from tests.builders.file import build_gzip_csv
from tests.builders.s3 import MockS3Object, MockS3
from tests.builders.spine import build_message

_SPINE_CSV_MAPPING = OrderedDict(
    [
        ("_time", lambda m: m.time.isoformat()),
        ("conversationID", lambda m: m.conversation_id),
        ("GUID", lambda m: m.guid),
        ("interactionID", lambda m: m.interaction_id),
        ("messageSender", lambda m: m.from_party_asid),
        ("messageRecipient", lambda m: m.to_party_asid),
        ("messageRef", lambda m: "NotProvided" if m.message_ref is None else m.message_ref),
        ("fromSystem", lambda m: m.from_system),
        ("toSystem", lambda m: m.to_system),
        ("jdiEvent", lambda m: "NONE" if m.error_code is None else str(m.error_code)),
    ]
)


def _spine_csv_gz(messages):
    return build_gzip_csv(
        header=_SPINE_CSV_MAPPING.keys(),
        rows=[[field(msg) for field in _SPINE_CSV_MAPPING.values()] for msg in messages],
    )


def test_read_spine_messages():

    spine_messages = [build_message() for _ in range(10)]

    mock_s3_conn = MockS3(
        objects=[
            MockS3Object(
                bucket="test_bucket",
                key="data/1.csv.gz",
                contents=_spine_csv_gz(messages=spine_messages[:4]),
            ),
            MockS3Object(
                bucket="test_bucket",
                key="data/2.csv.gz",
                contents=_spine_csv_gz(messages=spine_messages[4:]),
            ),
        ]
    )

    io = TransferClassifierIO(s3_data_manager=S3DataManager(mock_s3_conn))

    actual = io.read_spine_messages(
        ["s3://test_bucket/data/1.csv.gz", "s3://test_bucket/data/2.csv.gz"]
    )

    assert list(actual) == spine_messages
