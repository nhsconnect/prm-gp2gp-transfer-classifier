from datetime import timedelta

from prmdata.domain.gp2gp.transfer import Practice, Transfer
from prmdata.domain.gp2gp.transfer_outcome import (
    TransferFailureReason,
    TransferOutcome,
    TransferStatus,
)
from tests.builders.common import a_datetime, a_duration, a_string

THREE_DAYS_IN_SECONDS = 259200
EIGHT_DAYS_IN_SECONDS = 691200


def build_practice(**kwargs) -> Practice:
    return Practice(
        asid=kwargs.get("asid", a_string(12)),
        supplier=kwargs.get("supplier", a_string(12)),
        ods_code=kwargs.get("ods_code", a_string(4)),
        name=kwargs.get("name", a_string(15)),
        sicbl_ods_code=kwargs.get("sicbl_ods_code", a_string(4)),
        sicbl_name=kwargs.get("sicbl_name", a_string(15)),
    )


def build_transfer(**kwargs) -> Transfer:
    return Transfer(
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        sla_duration=kwargs.get("sla_duration", a_duration()),
        requesting_practice=kwargs.get("requesting_practice", build_practice()),
        sending_practice=kwargs.get("sending_practice", build_practice()),
        sender_error_codes=kwargs.get("sender_error_codes", []),
        final_error_codes=kwargs.get("final_error_codes", []),
        intermediate_error_codes=kwargs.get("intermediate_error_codes", []),
        outcome=kwargs.get(
            "outcome",
            TransferOutcome(status=TransferStatus.INTEGRATED_ON_TIME, failure_reason=None),
        ),
        date_requested=kwargs.get("date_requested", a_datetime()),
        date_completed=kwargs.get("date_completed", None),
        last_sender_message_timestamp=None,
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


def a_transfer_where_no_core_ehr_was_sent():
    return build_transfer(
        outcome=TransferOutcome(
            status=TransferStatus.TECHNICAL_FAILURE,
            failure_reason=TransferFailureReason.CORE_EHR_NOT_SENT,
        )
    )
