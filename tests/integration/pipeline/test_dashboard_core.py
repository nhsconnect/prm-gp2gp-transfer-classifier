from datetime import datetime, timedelta

from dateutil.tz import UTC, tzutc
from freezegun import freeze_time

from gp2gp.dashboard.national_data import (
    NationalDataPlatformData,
    DataPlatformIntegratedMetrics,
    DataPlatformNationalMetrics,
    DataPlatformPaperMetrics,
)
from gp2gp.dashboard.practice_metrics import (
    IntegratedPracticeMetrics,
    RequesterMetrics,
    MonthlyMetrics,
    PracticeSummary,
    PracticeMetricsData,
)
from gp2gp.date.range import DateTimeRange
from gp2gp.odsportal.models import PracticeDetails
from gp2gp.pipeline.dashboard.core import (
    calculate_practice_metrics_data,
    parse_transfers_from_messages,
    calculate_national_metrics_data,
)
from gp2gp.service.common import EIGHT_DAYS_IN_SECONDS, THREE_DAYS_IN_SECONDS

from gp2gp.service.transfer import Transfer, TransferStatus

from tests.builders.spine import build_message
from tests.builders.service import build_transfer


def _build_successful_conversation(**kwargs):
    return [
        build_message(
            time=kwargs.get("ehr_request_started_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN010000UK05",
            from_party_asid=kwargs.get("requesting_asid"),
            to_party_asid=kwargs.get("sending_asid"),
        ),
        build_message(
            time=kwargs.get("ehr_request_completed_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc_1",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN030000UK06",
            from_party_asid=kwargs.get("sending_asid"),
            to_party_asid=kwargs.get("requesting_asid"),
        ),
        build_message(
            time=kwargs.get("ehr_request_started_acknowledged_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc_2",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid=kwargs.get("sending_asid"),
            to_party_asid=kwargs.get("requesting_asid"),
            message_ref="abc",
        ),
        build_message(
            time=kwargs.get("ehr_request_completed_acknowledged_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc_3",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid=kwargs.get("requesting_asid"),
            to_party_asid=kwargs.get("sending_asid"),
            message_ref="abc_1",
        ),
    ]


@freeze_time(datetime(year=2020, month=1, day=15, hour=23, second=42), tz_offset=0)
def test_parses_transfer_correctly_given_valid_message_list():
    time_range = DateTimeRange(
        start=datetime(2019, 12, 1, tzinfo=UTC), end=datetime(2020, 1, 1, tzinfo=UTC)
    )

    requesting_asid_with_transfer = "343434343434"
    sending_asid_with_transfer = "111134343434"
    conversation_id = "abcdefg_1234"

    spine_messages = _build_successful_conversation(
        conversation_id=conversation_id,
        requesting_asid=requesting_asid_with_transfer,
        sending_asid=sending_asid_with_transfer,
        ehr_request_started_on=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
        ehr_request_completed_on=datetime(2019, 12, 30, 18, 3, 21, tzinfo=UTC),
        ehr_request_started_acknowledged_on=datetime(2019, 12, 30, 18, 3, 23, tzinfo=UTC),
        ehr_request_completed_acknowledged_on=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
    )

    expected = [
        Transfer(
            conversation_id=conversation_id,
            sla_duration=timedelta(days=1, seconds=52707),
            requesting_practice_asid=requesting_asid_with_transfer,
            sending_practice_asid=sending_asid_with_transfer,
            status=TransferStatus.INTEGRATED,
            date_requested=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
            date_completed=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
            final_error_code=None,
            intermediate_error_codes=[],
        )
    ]

    actual = list(parse_transfers_from_messages(spine_messages, time_range))

    assert actual == expected


@freeze_time(datetime(year=2020, month=1, day=15, hour=23, second=42), tz_offset=0)
def test_calculates_correct_metrics_given_a_successful_transfer():
    time_range = DateTimeRange(
        start=datetime(2019, 12, 1, tzinfo=UTC), end=datetime(2020, 1, 1, tzinfo=UTC)
    )

    requesting_practice_name = "Test GP"
    requesting_ods_code = "A12345"
    requesting_asid_with_transfer = "343434343434"
    sending_asid_with_transfer = "111134343434"
    conversation_id = "abcdefg_1234"

    transfers = [
        Transfer(
            conversation_id=conversation_id,
            sla_duration=timedelta(days=1, seconds=52707),
            requesting_practice_asid=requesting_asid_with_transfer,
            sending_practice_asid=sending_asid_with_transfer,
            status=TransferStatus.INTEGRATED,
            date_requested=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
            date_completed=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
            final_error_code=None,
            intermediate_error_codes=[],
        )
    ]

    practice_list = [
        PracticeDetails(
            asids=[requesting_asid_with_transfer],
            ods_code=requesting_ods_code,
            name=requesting_practice_name,
        )
    ]

    expected = PracticeMetricsData(
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
                            integrated=IntegratedPracticeMetrics(
                                transfer_count=1,
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

    actual = calculate_practice_metrics_data(transfers, practice_list, time_range)

    assert actual == expected


@freeze_time(datetime(year=2020, month=1, day=17, hour=21, second=32), tz_offset=0)
def test_calculates_correct_national_metrics_given_series_of_messages():
    sla_duration_within_3_days = timedelta(seconds=THREE_DAYS_IN_SECONDS)
    sla_duration_within_8_days = timedelta(seconds=EIGHT_DAYS_IN_SECONDS)
    sla_duration_beyond_8_days = timedelta(seconds=EIGHT_DAYS_IN_SECONDS + 1)

    transfers = [
        build_transfer(),
        build_transfer(status=TransferStatus.INTEGRATED, sla_duration=sla_duration_within_3_days),
        build_transfer(status=TransferStatus.INTEGRATED, sla_duration=sla_duration_within_8_days),
        build_transfer(status=TransferStatus.INTEGRATED, sla_duration=sla_duration_within_8_days),
        build_transfer(status=TransferStatus.INTEGRATED, sla_duration=sla_duration_beyond_8_days),
        build_transfer(status=TransferStatus.INTEGRATED, sla_duration=sla_duration_beyond_8_days),
        build_transfer(status=TransferStatus.INTEGRATED, sla_duration=sla_duration_beyond_8_days),
    ]

    time_range = DateTimeRange(
        start=datetime(2019, 12, 1, tzinfo=UTC), end=datetime(2020, 1, 1, tzinfo=UTC)
    )
    current_datetime = datetime.now(tzutc())

    expected_national_metrics_by_month = DataPlatformNationalMetrics(
        transfer_count=7,
        integrated=DataPlatformIntegratedMetrics(
            transfer_percentage=85.71,
            transfer_count=6,
            within_3_days=1,
            within_8_days=2,
            beyond_8_days=3,
        ),
        paper_fallback=DataPlatformPaperMetrics(transfer_count=4, transfer_percentage=57.14),
        year=2019,
        month=12,
    )

    expected = NationalDataPlatformData(
        generated_on=current_datetime, metrics=[expected_national_metrics_by_month]
    )
    actual = calculate_national_metrics_data(transfers, time_range)

    assert actual == expected
