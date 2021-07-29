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
from prmdata.pipeline.platform_metrics_calculator.core import (
    parse_transfers_from_messages,
    calculate_national_metrics_data,
)

from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    TransferOutcome,
    Practice,
)
from prmdata.utils.reporting_window import MonthlyReportingWindow

from tests.builders.spine import build_message
from tests.builders.gp2gp import (
    a_transfer_integrated_beyond_8_days,
    a_transfer_integrated_within_3_days,
    a_transfer_integrated_between_3_and_8_days,
    a_transfer_with_a_final_error,
    a_transfer_that_was_never_integrated,
    a_transfer_where_the_request_was_never_acknowledged,
    a_transfer_where_no_core_ehr_was_sent,
    a_transfer_where_no_copc_continue_was_sent,
    a_transfer_where_copc_fragments_were_required_but_not_sent,
    a_transfer_where_copc_fragments_remained_unacknowledged,
    a_transfer_where_the_sender_reported_an_unrecoverable_error,
    a_transfer_where_a_copc_triggered_an_error,
)


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
    conversation_id = "abcdefg_1234"
    requesting_practice = Practice(asid="343434343434", supplier="SupplierA")
    sending_practice = Practice(asid="111134343434", supplier="SupplierB")

    spine_messages = _build_successful_conversation(
        conversation_id=conversation_id,
        requesting_asid=requesting_practice.asid,
        sending_asid=sending_practice.asid,
        requesting_supplier=requesting_practice.supplier,
        sending_supplier=sending_practice.supplier,
        ehr_request_started_on=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
        ehr_request_completed_on=datetime(2019, 12, 30, 18, 3, 21, tzinfo=UTC),
        ehr_request_started_acknowledged_on=datetime(2019, 12, 30, 18, 3, 23, tzinfo=UTC),
        ehr_request_completed_acknowledged_on=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
    )

    expected = [
        Transfer(
            conversation_id=conversation_id,
            sla_duration=timedelta(days=1, seconds=52707),
            requesting_practice=requesting_practice,
            sending_practice=sending_practice,
            outcome=TransferOutcome(failure_reason=None, status=TransferStatus.INTEGRATED_ON_TIME),
            date_requested=datetime(2019, 12, 30, 18, 2, 29, tzinfo=UTC),
            date_completed=datetime(2020, 1, 1, 8, 41, 48, tzinfo=UTC),
            sender_error_codes=[None],
            final_error_codes=[None],
            intermediate_error_codes=[],
        )
    ]

    actual = list(parse_transfers_from_messages(spine_messages, reporting_window))

    assert actual == expected


@freeze_time(datetime(year=2020, month=1, day=17, hour=21, second=32), tz_offset=0)
def test_calculates_correct_national_metrics_given_series_of_messages():

    transfers = [
        a_transfer_that_was_never_integrated(),
        a_transfer_where_the_request_was_never_acknowledged(),
        a_transfer_where_no_core_ehr_was_sent(),
        a_transfer_where_no_copc_continue_was_sent(),
        a_transfer_where_copc_fragments_were_required_but_not_sent(),
        a_transfer_where_copc_fragments_remained_unacknowledged(),
        a_transfer_where_the_sender_reported_an_unrecoverable_error(),
        a_transfer_where_a_copc_triggered_an_error(),
        a_transfer_integrated_within_3_days(),
        a_transfer_integrated_between_3_and_8_days(),
        a_transfer_integrated_between_3_and_8_days(),
        a_transfer_integrated_beyond_8_days(),
        a_transfer_integrated_beyond_8_days(),
        a_transfer_integrated_beyond_8_days(),
        a_transfer_with_a_final_error(),
    ]

    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(2019, 12, 1, tzinfo=UTC),
        overflow_month_start=datetime(2020, 1, 1, tzinfo=UTC),
    )
    current_datetime = datetime.now(tzutc())

    expected_national_metrics = MonthlyNationalMetrics(
        transfer_count=15,
        integrated=IntegratedMetrics(
            transfer_percentage=40.0,
            transfer_count=6,
            within_3_days=1,
            within_8_days=2,
            beyond_8_days=3,
        ),
        failed=FailedMetrics(transfer_count=1, transfer_percentage=6.67),
        pending=PendingMetrics(transfer_count=8, transfer_percentage=53.33),
        paper_fallback=PaperFallbackMetrics(transfer_count=12, transfer_percentage=80.0),
        year=2019,
        month=12,
    )

    expected = NationalMetricsPresentation(
        generated_on=current_datetime, metrics=[expected_national_metrics]
    )
    actual = calculate_national_metrics_data(transfers, reporting_window)

    assert actual == expected
