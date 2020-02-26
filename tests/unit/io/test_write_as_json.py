import json
from dataclasses import dataclass
from io import StringIO

from gp2gp.io.json import write_as_json


def _assert_output_file_contains(outfile: StringIO, expected: str):
    outfile.seek(0)
    actual = outfile.read()
    assert actual == expected


def test_writes_to_json():
    @dataclass
    class PracticeDetailTest:
        ods_code: str

    @dataclass
    class PracticeListTest:
        practices: [PracticeDetailTest]

    data = PracticeListTest(practices=[PracticeDetailTest(ods_code="A12345")])
    outfile = StringIO()

    expected = json.dumps({"practices": [{"odsCode": "A12345"}]})
    write_as_json(data, outfile)

    _assert_output_file_contains(outfile, expected)
