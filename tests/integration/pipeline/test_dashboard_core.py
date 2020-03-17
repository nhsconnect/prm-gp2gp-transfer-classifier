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
from gp2gp.odsportal.models import PracticeDetails
from tests.builders.spine import build_message
from gp2gp.pipeline.dashboard.core import calculate_dashboard_data


@freeze_time(datetime(year=2020, month=1, day=15, hour=23, second=42), tz_offset=0)
def test_foobar():
    time_range = DateTimeRange(
        start=datetime(2019, 12, 1, tzinfo=UTC), end=datetime(2020, 1, 1, tzinfo=UTC)
    )

    spine_messages = [
        build_message(
            time=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
            conversation_id="abc",
            guid="abc",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN010000UK05",
            from_party_ods_code="A12345",
            to_party_ods_code="B56789",
        ),
        build_message(
            time=datetime(2019, 12, 30, 18, 3, 21, tzinfo=UTC),
            conversation_id="abc",
            guid="abc_1",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN030000UK06",
            from_party_ods_code="B56789",
            to_party_ods_code="A12345",
        ),
        build_message(
            time=datetime(2019, 12, 30, 18, 3, 23, tzinfo=UTC),
            conversation_id="abc",
            guid="abc_2",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_ods_code="B56789",
            to_party_ods_code="A12345",
            message_ref="abc",
        ),
        build_message(
            time=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
            conversation_id="abc",
            guid="abc_3",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_ods_code="A12345",
            to_party_ods_code="B56789",
            message_ref="abc_1",
        ),
    ]

    practice_list = [PracticeDetails(ods_code="A12345", name="Test GP")]

    expected = ServiceDashboardData(
        generated_on=datetime(year=2020, month=1, day=15, hour=23, second=42, tzinfo=UTC),
        practices=[
            PracticeSummary(
                ods_code="A12345",
                name="Test GP",
                metrics=[
                    MonthlyMetrics(
                        year=2019,
                        month=12,
                        requester=RequesterMetrics(
                            time_to_integrate_sla=TimeToIntegrateSla(
                                within_3_days=1, within_8_days=0, beyond_8_days=0,
                            )
                        ),
                    )
                ],
            )
        ],
    )

    actual = calculate_dashboard_data(spine_messages, practice_list, time_range)

    assert actual == expected
