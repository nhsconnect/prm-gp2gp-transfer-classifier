from gp2gp.io.json import read_json_file


def test_returns_dictionary(fs):
    file_path = "/foo/bar.json"
    fs.create_dir("/foo")
    fs.create_file(file_path, contents='{"status": "open"}')

    expected = {"status": "open"}
    actual = read_json_file(file_path)

    assert actual == expected
