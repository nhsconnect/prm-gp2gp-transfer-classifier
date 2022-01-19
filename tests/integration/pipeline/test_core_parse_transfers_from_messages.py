from datetime import datetime, timedelta

from dateutil.tz import UTC

from prmdata.domain.gp2gp.transfer import Practice, Transfer
from prmdata.domain.gp2gp.transfer_outcome import TransferOutcome, TransferStatus
from prmdata.domain.monthly_reporting_window import MonthlyReportingWindow
from prmdata.pipeline.parse_transfers_from_messages import parse_transfers_from_messages_monthly
from tests.builders.spine import build_message


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


def test_returns_transfers_given_valid_message_list():
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
        # core ehr/request completed sometimes gets sent before request started acknowledgement
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
            last_sender_message_timestamp=datetime(2019, 12, 30, 18, 3, 23, tzinfo=UTC),
        )
    ]

    actual = list(
        parse_transfers_from_messages_monthly(
            spine_messages=spine_messages,
            reporting_window=reporting_window,
            conversation_cutoff=timedelta(14),
        )
    )

    assert actual == expected
