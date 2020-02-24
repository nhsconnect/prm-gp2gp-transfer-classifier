import json
from datetime import datetime
from io import StringIO
from dateutil.tz import tzutc

from gp2gp.odsportal.models import PracticeList, PracticeDetails
from gp2gp.odsportal.sinks import write_practice_list_json


def _assert_output_file_contains(outfile: StringIO, expected: str):
    outfile.seek(0)
    actual = outfile.read()
    assert actual == expected


def test_write_service_dashboard_json_correctly_serializes_practices():
    dashboard_data = PracticeList(
        generated_on=datetime(year=2020, month=1, day=1, tzinfo=tzutc()),
        practices=[
            PracticeDetails(ods_code="A12345", name="GP Surgery"),
            PracticeDetails(ods_code="B12345", name="GP Surgery 2"),
        ],
    )
    outfile = StringIO()

    expected = json.dumps(
        {
            "generatedOn": "2020-01-01T00:00:00+00:00",
            "practices": [
                {"odsCode": "A12345", "name": "GP Surgery"},
                {"odsCode": "B12345", "name": "GP Surgery 2"},
            ],
        }
    )

    write_practice_list_json(dashboard_data, outfile)

    _assert_output_file_contains(outfile, expected)
