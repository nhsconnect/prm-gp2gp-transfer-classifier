from datetime import timedelta
from typing import List

from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    filter_for_successful_transfers,
    TransferOutcome,
    TransferFailureReason,
)
from tests.builders.gp2gp import build_transfer
from tests.builders.common import a_datetime

date_requested = a_datetime()
date_completed = a_datetime()


def test_includes_successful_transfer():
    integrated_transfer_outcome = TransferOutcome(
        status=TransferStatus.INTEGRATED_ON_TIME, reason=TransferFailureReason.DEFAULT
    )
    successful_transfer = build_transfer(
        conversation_id="123",
        sla_duration=timedelta(hours=1),
        requesting_practice_asid="121212121212",
        sending_practice_asid="343434343434",
        requesting_supplier="EMIS",
        sending_supplier="SystemOne",
        final_error_codes=[],
        transfer_outcome=integrated_transfer_outcome,
        date_requested=date_requested,
        date_completed=date_completed,
    )

    transfers = [successful_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [
        Transfer(
            conversation_id="123",
            sla_duration=timedelta(hours=1),
            requesting_practice_asid="121212121212",
            sending_practice_asid="343434343434",
            requesting_supplier="EMIS",
            sending_supplier="SystemOne",
            sender_error_code=None,
            final_error_codes=[],
            intermediate_error_codes=[],
            transfer_outcome=integrated_transfer_outcome,
            date_requested=date_requested,
            date_completed=date_completed,
        )
    ]

    assert list(actual) == expected


def test_includes_suppressed_transfers():
    integrated_transfer_outcome = TransferOutcome(
        status=TransferStatus.INTEGRATED_ON_TIME, reason=TransferFailureReason.DEFAULT
    )
    suppressed_transfer = build_transfer(
        conversation_id="456",
        sla_duration=timedelta(hours=2),
        requesting_practice_asid="121212121212",
        sending_practice_asid="343434343434",
        requesting_supplier="Vision",
        sending_supplier="SystemOne",
        final_error_codes=[15],
        transfer_outcome=integrated_transfer_outcome,
        date_requested=date_requested,
        date_completed=date_completed,
    )

    transfers = [suppressed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [
        Transfer(
            conversation_id="456",
            sla_duration=timedelta(hours=2),
            requesting_practice_asid="121212121212",
            sending_practice_asid="343434343434",
            requesting_supplier="Vision",
            sending_supplier="SystemOne",
            sender_error_code=None,
            final_error_codes=[15],
            intermediate_error_codes=[],
            transfer_outcome=integrated_transfer_outcome,
            date_requested=date_requested,
            date_completed=date_completed,
        )
    ]

    assert list(actual) == expected


def test_excludes_failed_transfers():
    integrated_transfer_outcome = TransferOutcome(
        status=TransferStatus.INTEGRATED_ON_TIME, reason=TransferFailureReason.DEFAULT
    )
    failed_transfer_outcome = TransferOutcome(
        status=TransferStatus.TECHNICAL_FAILURE, reason=TransferFailureReason.FINAL_ERROR
    )
    integrated_transfer_1 = build_transfer(transfer_outcome=integrated_transfer_outcome)
    integrated_transfer_2 = build_transfer(transfer_outcome=integrated_transfer_outcome)
    failed_transfer = build_transfer(transfer_outcome=failed_transfer_outcome)
    transfers = [integrated_transfer_1, integrated_transfer_2, failed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [integrated_transfer_1, integrated_transfer_2]

    assert list(actual) == expected


def test_excludes_transfers_missing_sla_duration():
    integrated_transfer_outcome = TransferOutcome(
        status=TransferStatus.INTEGRATED_ON_TIME, reason=TransferFailureReason.DEFAULT
    )
    failed_transfer_outcome = TransferOutcome(
        status=TransferStatus.TECHNICAL_FAILURE, reason=TransferFailureReason.FINAL_ERROR
    )
    integrated_transfer_1 = build_transfer(transfer_outcome=integrated_transfer_outcome)
    integrated_transfer_2 = build_transfer(
        transfer_outcome=integrated_transfer_outcome, sla_duration=None
    )
    failed_transfer = build_transfer(transfer_outcome=failed_transfer_outcome)
    transfers = [integrated_transfer_1, integrated_transfer_2, failed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [integrated_transfer_1]

    assert list(actual) == expected


def test_excludes_pending_transfers():
    pending_transfer = build_transfer()

    transfers = [pending_transfer]

    actual = filter_for_successful_transfers(transfers)
    expected: List[Transfer] = []

    assert list(actual) == expected
