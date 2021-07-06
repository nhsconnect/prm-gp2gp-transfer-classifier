from datetime import datetime, timedelta

from dateutil.tz import UTC, tzutc
from freezegun import freeze_time

from prmdata.domain.data_platform.national_metrics import (
    NationalMetricsPresentation,
    IntegratedMetrics,
    MonthlyNationalMetrics,
    PaperFallbackMetrics,
    FailedMetrics,
    PendingMetrics,
)
from prmdata.domain.data_platform.practice_metrics import (
    IntegratedPracticeMetrics,
    RequesterMetrics,
    MonthlyMetrics,
    PracticeSummary,
    PracticeMetricsPresentation,
)
from prmdata.domain.ods_portal.models import PracticeDetails, CcgDetails, OrganisationMetadata
from prmdata.pipeline.platform_metrics_calculator.core import (
    calculate_practice_metrics_data,
    parse_transfers_from_messages,
    calculate_national_metrics_data,
)
from prmdata.domain.gp2gp.sla import EIGHT_DAYS_IN_SECONDS, THREE_DAYS_IN_SECONDS

from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    TransferFailureReason,
    TransferOutcome,
)
from prmdata.utils.reporting_window import MonthlyReportingWindow

from tests.builders.spine import build_message
from tests.builders.gp2gp import build_transfer


def _build_successful_conversation(**kwargs):
    return [
        build_message(
            time=kwargs.get("ehr_request_started_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN010000UK05",
            from_party_asid=kwargs.get("requesting_asid"),
            to_party_asid=kwargs.get("sending_asid"),
            from_system=kwargs.get("requesting_supplier"),
            to_system=kwargs.get("sending_supplier"),
        ),
        build_message(
            time=kwargs.get("ehr_request_completed_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc_1",
            interaction_id="urn:nhs:names:services:gp2gp/RCMR_IN030000UK06",
            from_party_asid=kwargs.get("sending_asid"),
            to_party_asid=kwargs.get("requesting_asid"),
            from_system=kwargs.get("requesting_supplier"),
            to_system=kwargs.get("sending_supplier"),
        ),
        build_message(
            time=kwargs.get("ehr_request_started_acknowledged_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc_2",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid=kwargs.get("sending_asid"),
            to_party_asid=kwargs.get("requesting_asid"),
            from_system=kwargs.get("requesting_supplier"),
            to_system=kwargs.get("sending_supplier"),
            message_ref="abc",
        ),
        build_message(
            time=kwargs.get("ehr_request_completed_acknowledged_on"),
            conversation_id=kwargs.get("conversation_id"),
            guid="abc_3",
            interaction_id="urn:nhs:names:services:gp2gp/MCCI_IN010000UK13",
            from_party_asid=kwargs.get("requesting_asid"),
            to_party_asid=kwargs.get("sending_asid"),
            from_system=kwargs.get("requesting_supplier"),
            to_system=kwargs.get("sending_supplier"),
            message_ref="abc_1",
        ),
    ]


@freeze_time(datetime(year=2020, month=1, day=15, hour=23, second=42), tz_offset=0)
def test_parses_transfer_correctly_given_valid_message_list():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(2019, 12, 1, tzinfo=UTC),
        overflow_month_start=datetime(2020, 1, 1, tzinfo=UTC),
    )

    requesting_asid_with_transfer = "343434343434"
    sending_asid_with_transfer = "111134343434"
    requesting_supplier = "EMIS"
    sending_supplier = "Vision"
    conversation_id = "abcdefg_1234"

    spine_messages = _build_successful_conversation(
        conversation_id=conversation_id,
        requesting_asid=requesting_asid_with_transfer,
        sending_asid=sending_asid_with_transfer,
        requesting_supplier=requesting_supplier,
        sending_supplier=sending_supplier,
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
            requesting_supplier=requesting_supplier,
            sending_supplier=sending_supplier,
            transfer_outcome=TransferOutcome(
                reason=TransferFailureReason.DEFAULT, status=TransferStatus.INTEGRATED_ON_TIME
            ),
            date_requested=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
            date_completed=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
            sender_error_code=None,
            final_error_codes=[None],
            intermediate_error_codes=[],
        )
    ]

    actual = list(parse_transfers_from_messages(spine_messages, reporting_window))

    assert actual == expected


@freeze_time(datetime(year=2020, month=1, day=15, hour=23, second=42), tz_offset=0)
def test_calculates_correct_metrics_given_a_successful_transfer():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(2019, 12, 1, tzinfo=UTC),
        overflow_month_start=datetime(2020, 1, 1, tzinfo=UTC),
    )

    requesting_practice_name = "Test GP"
    requesting_ods_code = "A12345"
    requesting_asid_with_transfer = "343434343434"
    sending_asid_with_transfer = "111134343434"
    requesting_supplier = "SystemOne"
    sending_supplier = "Unknown"
    conversation_id = "abcdefg_1234"
    ccg_ods_code = "23B"
    ccg_name = "Test CCG"

    transfers = [
        Transfer(
            conversation_id=conversation_id,
            sla_duration=timedelta(days=1, seconds=52707),
            requesting_practice_asid=requesting_asid_with_transfer,
            sending_practice_asid=sending_asid_with_transfer,
            requesting_supplier=requesting_supplier,
            sending_supplier=sending_supplier,
            transfer_outcome=TransferOutcome(
                reason=TransferFailureReason.DEFAULT, status=TransferStatus.INTEGRATED_ON_TIME
            ),
            date_requested=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
            date_completed=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
            sender_error_code=None,
            final_error_codes=[],
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

    ccg_list = [CcgDetails(name=ccg_name, ods_code=ccg_ods_code, practices=[requesting_ods_code])]

    organisation_metadata = OrganisationMetadata(
        generated_on=datetime(year=2020, month=1, day=15, hour=23, second=42, tzinfo=UTC),
        practices=practice_list,
        ccgs=ccg_list,
    )

    expected = PracticeMetricsPresentation(
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
                                within_3_days_percentage=100,
                                within_8_days_percentage=0,
                                beyond_8_days_percentage=0,
                            ),
                        ),
                    )
                ],
            )
        ],
        ccgs=ccg_list,
    )

    actual = calculate_practice_metrics_data(transfers, organisation_metadata, reporting_window)

    assert actual == expected


@freeze_time(datetime(year=2020, month=1, day=17, hour=21, second=32), tz_offset=0)
def test_calculates_correct_national_metrics_given_series_of_messages():
    sla_duration_within_3_days = timedelta(seconds=THREE_DAYS_IN_SECONDS)
    sla_duration_within_8_days = timedelta(seconds=EIGHT_DAYS_IN_SECONDS)
    sla_duration_beyond_8_days = timedelta(seconds=EIGHT_DAYS_IN_SECONDS + 1)

    integrated_late_transfer_outcome = TransferOutcome(
        status=TransferStatus.PROCESS_FAILURE, reason=TransferFailureReason.INTEGRATED_LATE
    )
    integrated_on_time_transfer_outcome = TransferOutcome(
        status=TransferStatus.INTEGRATED_ON_TIME, reason=TransferFailureReason.DEFAULT
    )
    pending_with_error_transfer_outcome = TransferOutcome(
        status=TransferStatus.PENDING_WITH_ERROR, reason=TransferFailureReason.DEFAULT
    )
    failed_transfer_outcome = TransferOutcome(
        status=TransferStatus.FAILED, reason=TransferFailureReason.DEFAULT
    )

    transfers = [
        build_transfer(),
        build_transfer(transfer_outcome=pending_with_error_transfer_outcome),
        build_transfer(
            transfer_outcome=integrated_on_time_transfer_outcome,
            sla_duration=sla_duration_within_3_days,
        ),
        build_transfer(
            transfer_outcome=integrated_on_time_transfer_outcome,
            sla_duration=sla_duration_within_8_days,
        ),
        build_transfer(
            transfer_outcome=integrated_on_time_transfer_outcome,
            sla_duration=sla_duration_within_8_days,
        ),
        build_transfer(
            transfer_outcome=integrated_late_transfer_outcome,
            sla_duration=sla_duration_beyond_8_days,
        ),
        build_transfer(
            transfer_outcome=integrated_late_transfer_outcome,
            sla_duration=sla_duration_beyond_8_days,
        ),
        build_transfer(
            transfer_outcome=integrated_late_transfer_outcome,
            sla_duration=sla_duration_beyond_8_days,
        ),
        build_transfer(transfer_outcome=failed_transfer_outcome),
    ]

    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(2019, 12, 1, tzinfo=UTC),
        overflow_month_start=datetime(2020, 1, 1, tzinfo=UTC),
    )
    current_datetime = datetime.now(tzutc())

    expected_national_metrics = MonthlyNationalMetrics(
        transfer_count=9,
        integrated=IntegratedMetrics(
            transfer_percentage=66.67,
            transfer_count=6,
            within_3_days=1,
            within_8_days=2,
            beyond_8_days=3,
        ),
        failed=FailedMetrics(transfer_count=1, transfer_percentage=11.11),
        pending=PendingMetrics(transfer_count=2, transfer_percentage=22.22),
        paper_fallback=PaperFallbackMetrics(transfer_count=6, transfer_percentage=66.67),
        year=2019,
        month=12,
    )

    expected = NationalMetricsPresentation(
        generated_on=current_datetime, metrics=[expected_national_metrics]
    )
    actual = calculate_national_metrics_data(transfers, reporting_window)

    assert actual == expected
