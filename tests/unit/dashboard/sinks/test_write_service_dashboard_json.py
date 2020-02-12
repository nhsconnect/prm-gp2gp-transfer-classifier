import json
from datetime import datetime
from io import StringIO

from dateutil.tz import tzutc

from gp2gp.dashboard.models import (
    ServiceDashboardData,
    PracticeSummary,
    MonthlyMetrics,
    RequesterMetrics,
    TimeToIntegrateSla,
)

from gp2gp.dashboard.sinks import write_service_dashboard_json


def _assert_output_file_contains(outfile: StringIO, expected: str):
    outfile.seek(0)
    actual = outfile.read()
    assert actual == expected


def test_write_service_dashboard_json_correctly_serializes_generated_on():
    dashboard_data = ServiceDashboardData(
        generated_on=datetime(
            year=2020, month=1, day=1, hour=11, minute=0, second=7, microsecond=1, tzinfo=tzutc()
        ),
        practices=[],
    )
    outfile = StringIO()

    expected = json.dumps({"generatedOn": "2020-01-01T11:00:07.000001+00:00", "practices": []})

    write_service_dashboard_json(dashboard_data, outfile)

    _assert_output_file_contains(outfile, expected)


def test_write_service_dashboard_json_correctly_serializes_practices():
    dashboard_data = ServiceDashboardData(
        generated_on=datetime(year=2020, month=1, day=1, tzinfo=tzutc()),
        practices=[
            PracticeSummary(
                ods_code="A12345",
                metrics=[
                    MonthlyMetrics(
                        year=2020,
                        month=1,
                        requester=RequesterMetrics(
                            time_to_integrate_sla=TimeToIntegrateSla(
                                within_3_days=1, within_8_days=0, beyond_8_days=2
                            )
                        ),
                    )
                ],
            ),
        ],
    )
    outfile = StringIO()

    expected = json.dumps(
        {
            "generatedOn": "2020-01-01T00:00:00+00:00",
            "practices": [
                {
                    "odsCode": "A12345",
                    "metrics": [
                        {
                            "year": 2020,
                            "month": 1,
                            "requester": {
                                "timeToIntegrateSla": {
                                    "within3Days": 1,
                                    "within8Days": 0,
                                    "beyond8Days": 2,
                                }
                            },
                        }
                    ],
                }
            ],
        }
    )

    write_service_dashboard_json(dashboard_data, outfile)

    _assert_output_file_contains(outfile, expected)
