from prmdata.utils.io.csv import read_gzip_csv_files_deprecated
from tests.builders.file import build_gzip_csv


def test_loads_one_file(fs):
    file_path = "input.csv.gz"
    fs.create_file(
        file_path,
        contents=build_gzip_csv(
            header=["id", "message"],
            rows=[["A", "A message"], ["B", "B message"], ["C", "C message"]],
        ),
    )

    input_files = [file_path]

    expected = [
        {"id": "A", "message": "A message"},
        {"id": "B", "message": "B message"},
        {"id": "C", "message": "C message"},
    ]

    actual = read_gzip_csv_files_deprecated(input_files)

    assert list(actual) == expected


def test_loads_two_files(fs):
    file_path_one = "input1.csv.gz"
    file_path_two = "input2.csv.gz"
    fs.create_file(
        file_path_one,
        contents=build_gzip_csv(
            header=["id", "message"],
            rows=[["A", "A message"], ["B", "B message"], ["C", "C message"]],
        ),
    )

    fs.create_file(
        file_path_two,
        contents=build_gzip_csv(
            header=["id", "message"],
            rows=[["D", "D message"], ["E", "E message"]],
        ),
    )

    input_files = [file_path_one, file_path_two]

    expected = [
        {"id": "A", "message": "A message"},
        {"id": "B", "message": "B message"},
        {"id": "C", "message": "C message"},
        {"id": "D", "message": "D message"},
        {"id": "E", "message": "E message"},
    ]

    actual = read_gzip_csv_files_deprecated(input_files)

    assert list(actual) == expected
