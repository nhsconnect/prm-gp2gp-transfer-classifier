from io import BytesIO
import gzip
from datetime import datetime
from dateutil import tz
from gp2gp.sources.spine import read_spine_csv_gz
from gp2gp.models.spine import Message

_SPINE_CSV_FIELDS = [
    "_time",
    "conversationID",
    "GUID",
    "interactionID",
    "fromNACS",
    "toNACS",
    "messageRef",
    "jdiEvent",
]


def _a_spine_cvs_row(**kwargs):
    return [
        kwargs["with_time"],
        kwargs["with_conversation_id"],
        kwargs["with_guid"],
        kwargs["with_interaction_id"],
        kwargs["with_from_nacs"],
        kwargs["with_to_nacs"],
        kwargs["with_message_ref"],
        kwargs["with_jdi_event"],
        kwargs["with_raw"],
    ]


def _build_csv_contents(with_header, with_rows):
    def build_line(values):
        return ",".join(values)

    header_line = build_line(with_header)
    row_lines = [build_line(row) for row in with_rows]

    return "\n".join([header_line] + row_lines)


def _build_gzip_buffer(string):
    buffer = BytesIO()
    with gzip.open(buffer, "wt") as f:
        f.write(string)
    buffer.seek(0)
    return buffer


def test_read_spine_csv_gz():
    spine_csv_content = _build_csv_contents(
        with_header=_SPINE_CSV_FIELDS,
        with_rows=[
            _a_spine_cvs_row(
                with_time="2019-12-31T23:37:55.334+0000",
                with_conversation_id="3802F7D7-EDA8-483F-AFE1-E1C615116B89",
                with_guid="915E57C0-2C26-11EA-B3D8-48DF371F5668",
                with_interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
                with_from_nacs="C84063",
                with_to_nacs="C84065",
                with_message_ref="3802F7D7-EDA8-483F-AFE1-E1C615116B89",
                with_jdi_event="NONE",
                with_raw="",
            ),
            _a_spine_cvs_row(
                with_time="2019-12-31T22:16:02.249+0000",
                with_conversation_id="CD9DF846-AD3F-4FA6-A47A-12D32117127A",
                with_guid="2DCC8689-008B-423A-9D40-937AF235BCAF",
                with_interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
                with_from_nacs="L82018",
                with_to_nacs="L82014",
                with_message_ref="CD9DF846-AD3F-4FA6-A47A-12D32117127A",
                with_jdi_event="23",
                with_raw="",
            ),
        ],
    )
    buffer = _build_gzip_buffer(spine_csv_content)
    actual = read_spine_csv_gz(buffer)
    expected = [
        Message(
            time=datetime(2019, 12, 31, 23, 37, 55, 334000, tz.UTC),
            conversation_id="3802F7D7-EDA8-483F-AFE1-E1C615116B89",
            guid="915E57C0-2C26-11EA-B3D8-48DF371F5668",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_ods="C84063",
            to_party_ods="C84065",
            message_ref="3802F7D7-EDA8-483F-AFE1-E1C615116B89",
            error_code=None,
        ),
        Message(
            time=datetime(2019, 12, 31, 22, 16, 2, 249000, tz.UTC),
            conversation_id="CD9DF846-AD3F-4FA6-A47A-12D32117127A",
            guid="2DCC8689-008B-423A-9D40-937AF235BCAF",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_ods="L82018",
            to_party_ods="L82014",
            message_ref="CD9DF846-AD3F-4FA6-A47A-12D32117127A",
            error_code=23,
        ),
    ]

    assert list(actual) == expected
