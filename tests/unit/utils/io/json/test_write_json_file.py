from datetime import datetime
from pathlib import Path

from prmdata.utils.io.json import write_json_file


def test_writes_dictionary(fs):
    content = {"status": "open"}
    file_path = "/foo/bar.json"
    fs.create_dir("/foo")

    write_json_file(content, file_path)

    expected = '{"status": "open"}'
    actual = Path(file_path).read_text()

    assert actual == expected


def test_writes_dictionary_with_datetime(fs):
    content = {"timestamp": datetime(2020, 7, 23)}
    file_path = "/foo/bar.json"
    fs.create_dir("/foo")

    write_json_file(content, file_path)

    expected = '{"timestamp": "2020-07-23T00:00:00"}'
    actual = Path(file_path).read_text()

    assert actual == expected
