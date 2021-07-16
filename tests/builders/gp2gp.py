from datetime import timedelta

from prmdata.domain.gp2gp.practice_metrics import PracticeMetrics, IntegratedPracticeMetrics
from prmdata.domain.gp2gp.sla import EIGHT_DAYS_IN_SECONDS, THREE_DAYS_IN_SECONDS
from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    TransferOutcome,
    TransferFailureReason,
)
from tests.builders.common import a_string, a_duration, an_integer, a_datetime


def build_transfer(**kwargs) -> Transfer:
    return Transfer(
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        sla_duration=kwargs.get("sla_duration", a_duration()),
        requesting_practice_asid=kwargs.get("requesting_practice_asid", a_string(12)),
        sending_practice_asid=kwargs.get("sending_practice_asid", a_string(12)),
        requesting_supplier=kwargs.get("requesting_supplier", a_string(12)),
        sending_supplier=kwargs.get("sending_supplier", a_string(12)),
        sender_error_code=kwargs.get("sender_error_code", None),
        final_error_codes=kwargs.get("final_error_codes", []),
        intermediate_error_codes=kwargs.get("intermediate_error_codes", []),
        outcome=kwargs.get(
            "outcome",
            TransferOutcome(status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None),
        ),
        date_requested=kwargs.get("date_requested", a_datetime()),
        date_completed=kwargs.get("date_completed", None),
    )


def build_practice_metrics(**kwargs) -> PracticeMetrics:
    return PracticeMetrics(
        ods_code=kwargs.get("ods_code", a_string(6)),
        name=kwargs.get("name", a_string()),
        integrated=IntegratedPracticeMetrics(
            transfer_count=kwargs.get("transfer_count", an_integer()),
            within_3_days=kwargs.get("within_3_days", an_integer()),
            within_8_days=kwargs.get("within_8_days", an_integer()),
            beyond_8_days=kwargs.get("beyond_8_days", an_integer()),
        ),
    )


def an_integrated_transfer(**kwargs):
    return build_transfer(
        outcome=TransferOutcome(status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None),
        sla_duration=kwargs.get("sla_duration", a_duration(max_length=604800)),
    )


def a_supressed_transfer(**kwargs):
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.INTEGRATED_ON_TIME,
            failure_reason=None,
        ),
        final_error_codes=[15],
        sla_duration=kwargs.get("sla_duration", a_duration(max_length=604800)),
    )


def a_transfer_integrated_within_3_days():
    return build_transfer(
        outcome=TransferOutcome(status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None),
        sla_duration=timedelta(seconds=THREE_DAYS_IN_SECONDS),
    )


def a_transfer_integrated_between_3_and_8_days():
    return build_transfer(
        outcome=TransferOutcome(status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None),
        sla_duration=timedelta(seconds=THREE_DAYS_IN_SECONDS + 1),
    )


def a_transfer_integrated_beyond_8_days():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.PROCESS_FAILURE,
            failure_reason=TransferFailureReason.INTEGRATED_LATE,
        ),
        sla_duration=timedelta(seconds=EIGHT_DAYS_IN_SECONDS + 1),
    )


def a_transfer_with_a_final_error():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.FINAL_ERROR,
        )
    )


def a_transfer_that_was_never_integrated():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.PROCESS_FAILURE,
            failure_reason=TransferFailureReason.TRANSFERRED_NOT_INTEGRATED,
        )
    )


def a_transfer_where_the_request_was_never_acknowledged():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.REQUEST_NOT_ACKNOWLEDGED,
        )
    )


def a_transfer_where_no_core_ehr_was_sent():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.CORE_EHR_NOT_SENT,
        )
    )


def a_transfer_where_no_large_message_continue_was_sent():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.COPC_NOT_SENT,
        )
    )


def a_transfer_where_large_messages_were_required_but_not_sent():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.COPC_NOT_SENT,
        )
    )


def a_transfer_where_large_messages_remained_unacknowledged():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.COPC_NOT_ACKNOWLEDGED,
        )
    )


def a_transfer_where_the_sender_reported_an_unrecoverable_error():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.FATAL_SENDER_ERROR,
        )
    )


def a_transfer_where_a_large_message_triggered_an_error():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.UNCLASSIFIED_FAILURE,
            failure_reason=TransferFailureReason.TRANSFERRED_NOT_INTEGRATED_WITH_ERROR,
        )
    )
