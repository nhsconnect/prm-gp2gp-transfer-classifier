from io import BytesIO
import gzip
from datetime import datetime
from dateutil import tz
from gp2gp.sources.spine import read_spine_csv_gz
from gp2gp.models.spine import Message


def build_csv_contents(
    with_header, with_rows,
):
    def build_line(values):
        return ",".join(values)

    header_line = build_line(with_header)
    row_lines = [build_line(row) for row in with_rows]

    return "\n".join([header_line] + row_lines)


def build_gzip_buffer(string):
    buffer = BytesIO()
    with gzip.open(buffer, "wt") as f:
        f.write(string)
    buffer.seek(0)
    return buffer


def test_read_spine_csv_gz():
    spine_csv_content = build_csv_contents(
        with_header=["_time", "conversationID", "GUID"],
        with_rows=[
            [
                "2019-12-31T23:37:55.334+0000",
                "3802F7D7-EDA8-483F-AFE1-E1C615116B89",
                "15E57C0-2C26-11EA-B3D8-48DF371F5668",
            ],
            [
                "2019-12-31T22:16:02.249+0000",
                "CD9DF846-AD3F-4FA6-A47A-12D32117127A",
                "2DCC8689-008B-423A-9D40-937AF235BCAF",
            ],
        ],
    )
    buffer = build_gzip_buffer(spine_csv_content)
    actual = read_spine_csv_gz(buffer)
    expected = [
        Message(
            time=datetime(2019, 12, 31, 23, 37, 55, 334000, tz.UTC),
            conversation_id="3802F7D7-EDA8-483F-AFE1-E1C615116B89",
            guid="15E57C0-2C26-11EA-B3D8-48DF371F5668",
        ),
        Message(
            time=datetime(2019, 12, 31, 22, 16, 2, 249000, tz.UTC),
            conversation_id="CD9DF846-AD3F-4FA6-A47A-12D32117127A",
            guid="2DCC8689-008B-423A-9D40-937AF235BCAF",
        ),
    ]

    assert list(actual) == expected
