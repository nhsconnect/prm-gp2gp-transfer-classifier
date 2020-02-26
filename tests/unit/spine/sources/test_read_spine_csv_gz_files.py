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
