from pathlib import Path

from gp2gp.io.file import write_to_text_file


def test_writes_json_string_to_file(fs):
    content = '{ "status": "open" }'
    file_path = "/foo/bar.json"
    fs.create_dir("/foo")
    write_to_text_file(content, file_path)

    actual = Path(file_path).read_text()

    assert actual == content
