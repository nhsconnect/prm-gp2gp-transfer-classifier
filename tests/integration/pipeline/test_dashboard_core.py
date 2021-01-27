from datetime import datetime

from dateutil.tz import UTC
from freezegun import freeze_time
from gp2gp.dashboard.models import (
    ServiceDashboardData,
    PracticeSummary,
    MonthlyMetrics,
    RequesterMetrics,
    TimeToIntegrateSla,
)
from gp2gp.date.range import DateTimeRange
from gp2gp.odsportal.models import OrganisationDetailsWithAsid
from gp2gp.pipeline.dashboard.core import calculate_dashboard_data

from tests.builders.spine import build_message


def _build_successful_conversation(**kwargs):
    return [
        build_message(
            time=kwargs.get("ehr_request_started_on"),
            conversation_id="abc",
            guid="abc",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN010000UK05",
            from_party_asid=kwargs.get("requesting_asid"),
            to_party_asid="123456789012",
        ),
        build_message(
            time=kwargs.get("ehr_request_completed_on"),
            conversation_id="abc",
            guid="abc_1",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN030000UK06",
            from_party_asid="123456789012",
            to_party_asid=kwargs.get("requesting_asid"),
        ),
        build_message(
            time=kwargs.get("ehr_request_started_acknowledged_on"),
            conversation_id="abc",
            guid="abc_2",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid="123456789012",
            to_party_asid=kwargs.get("requesting_asid"),
            message_ref="abc",
        ),
        build_message(
            time=kwargs.get("ehr_request_completed_acknowledged_on"),
            conversation_id="abc",
            guid="abc_3",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid=kwargs.get("requesting_asid"),
            to_party_asid="123456789012",
            message_ref="abc_1",
        ),
    ]


@freeze_time(datetime(year=2020, month=1, day=15, hour=23, second=42), tz_offset=0)
def test_calculates_correct_metrics_given_a_successful_transfer():
    time_range = DateTimeRange(
        start=datetime(2019, 12, 1, tzinfo=UTC), end=datetime(2020, 1, 1, tzinfo=UTC)
    )

    requesting_practice_name = "Test GP"
    requesting_ods_code = "A12345"
    requesting_asid = "343434343434"

    spine_messages = _build_successful_conversation(
        requesting_asid=requesting_asid,
        ehr_request_started_on=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
        ehr_request_completed_on=datetime(2019, 12, 30, 18, 3, 21, tzinfo=UTC),
        ehr_request_started_acknowledged_on=datetime(2019, 12, 30, 18, 3, 23, tzinfo=UTC),
        ehr_request_completed_acknowledged_on=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
    )

    practice_list = [
        OrganisationDetailsWithAsid(
            asid=requesting_asid, ods_code=requesting_ods_code, name=requesting_practice_name
        )
    ]

    expected = ServiceDashboardData(
        generated_on=datetime(year=2020, month=1, day=15, hour=23, second=42, tzinfo=UTC),
        practices=[
            PracticeSummary(
                ods_code=requesting_ods_code,
                name=requesting_practice_name,
                metrics=[
                    MonthlyMetrics(
                        year=2019,
                        month=12,
                        requester=RequesterMetrics(
                            time_to_integrate_sla=TimeToIntegrateSla(
                                within_3_days=1,
                                within_8_days=0,
                                beyond_8_days=0,
                            )
                        ),
                    )
                ],
            )
        ],
    )

    actual = calculate_dashboard_data(spine_messages, practice_list, time_range)

    assert actual == expected
