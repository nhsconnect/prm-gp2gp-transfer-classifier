from datetime import datetime

from dateutil.tz import tzutc

from gp2gp.spine.models import Message
from gp2gp.spine.sources import read_spine_csv_gz_files
from tests.builders.file import build_gzip_csv
from tests.builders.spine import SPINE_CSV_FIELDS, build_spine_csv_row


def _assert_message_guids(actual, expected):
    assert [m.guid for m in actual] == expected


def test_read_spine_csv_gz_files_loads_one_file(fs):
    message_file_path = "messages.csv.gz"
    fs.create_file(
        message_file_path,
        contents=build_gzip_csv(
            header=SPINE_CSV_FIELDS,
            rows=[
                build_spine_csv_row(guid="a"),
                build_spine_csv_row(guid="b"),
                build_spine_csv_row(guid="c"),
            ],
        ),
    )

    input_files = [message_file_path]

    expected_guids = ["a", "b", "c"]

    actual = read_spine_csv_gz_files(input_files)

    _assert_message_guids(actual, expected_guids)


def test_read_spine_csv_gz_files_loads_two_files(fs):
    message_file_path_one = "messages_one.csv.gz"
    message_file_path_two = "messages_two.csv.gz"
    fs.create_file(
        message_file_path_one,
        contents=build_gzip_csv(
            header=SPINE_CSV_FIELDS,
            rows=[
                build_spine_csv_row(guid="a"),
                build_spine_csv_row(guid="b"),
                build_spine_csv_row(guid="c"),
            ],
        ),
    )
    fs.create_file(
        message_file_path_two,
        contents=build_gzip_csv(
            header=SPINE_CSV_FIELDS,
            rows=[
                build_spine_csv_row(guid="d"),
                build_spine_csv_row(guid="e"),
                build_spine_csv_row(guid="f"),
            ],
        ),
    )

    input_files = [message_file_path_one, message_file_path_two]

    expected_guids = ["a", "b", "c", "d", "e", "f"]

    actual = read_spine_csv_gz_files(input_files)

    _assert_message_guids(actual, expected_guids)


def test_read_spine_csv_file_load_full_rows(fs):

    message_file_path = "messages.csv.gz"
    fs.create_file(
        message_file_path,
        contents=build_gzip_csv(
            header=SPINE_CSV_FIELDS,
            rows=[
                build_spine_csv_row(
                    time="2019-12-31T23:37:55.334+0000",
                    conversation_id="convo_abc",
                    guid="message_a",
                    interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
                    from_nacs="A12345",
                    to_nacs="B56789",
                    message_ref="NotProvided",
                    jdi_event="NONE",
                    raw="",
                ),
                build_spine_csv_row(
                    time="2019-12-31T22:16:02.249+0000",
                    conversation_id="convo_xyz",
                    guid="message_b",
                    interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
                    from_nacs="C12345",
                    to_nacs="D56789",
                    message_ref="convo_xyz",
                    jdi_event="23",
                    raw="",
                ),
            ],
        ),
    )

    input_files = [message_file_path]

    expected = [
        Message(
            time=datetime(2019, 12, 31, 23, 37, 55, 334000, tzutc()),
            conversation_id="convo_abc",
            guid="message_a",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_ods_code="A12345",
            to_party_ods_code="B56789",
            message_ref=None,
            error_code=None,
        ),
        Message(
            time=datetime(2019, 12, 31, 22, 16, 2, 249000, tzutc()),
            conversation_id="convo_xyz",
            guid="message_b",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_ods_code="C12345",
            to_party_ods_code="D56789",
            message_ref="convo_xyz",
            error_code=23,
        ),
    ]

    actual = read_spine_csv_gz_files(input_files)

    assert list(actual) == expected
