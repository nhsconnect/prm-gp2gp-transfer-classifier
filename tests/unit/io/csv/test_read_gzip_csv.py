from gp2gp.io.csv import read_gzip_csv
from tests.builders.file import build_gzip_csv


def test_returns_csv_row_as_dictionary(fs):
    file_path = "input.csv.gz"
    fs.create_file(
        file_path,
        contents=build_gzip_csv(
            header=["id", "message", "comment"],
            rows=[
                ["123", "A message", "A comment"],
                ["321", "Another message", "Another comment"]
            ],
        ),
    )

    expected = [{
        "id": "123",
        "message": "A message",
        "comment": "A comment"
    },
        {
            "id": "321",
            "message": "Another message",
            "comment": "Another comment"
        }
    ]

    actual = read_gzip_csv(file_path)

    assert list(actual) == expected
