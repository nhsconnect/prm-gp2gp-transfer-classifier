from datetime import timedelta

from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    filter_for_successful_transfers,
)
from tests.builders.gp2gp import build_transfer
from tests.builders.common import a_datetime

date_requested = a_datetime()
date_completed = a_datetime()


def test_includes_successful_transfer():
    successful_transfer = build_transfer(
        conversation_id="123",
        sla_duration=timedelta(hours=1),
        requesting_practice_asid="121212121212",
        sending_practice_asid="343434343434",
        final_error_code=None,
        status=TransferStatus.INTEGRATED,
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
            final_error_code=None,
            intermediate_error_codes=[],
            status=TransferStatus.INTEGRATED,
            date_requested=date_requested,
            date_completed=date_completed,
        )
    ]

    assert list(actual) == expected


def test_includes_suppressed_transfers():
    suppressed_transfer = build_transfer(
        conversation_id="456",
        sla_duration=timedelta(hours=2),
        requesting_practice_asid="121212121212",
        sending_practice_asid="343434343434",
        final_error_code=15,
        status=TransferStatus.INTEGRATED,
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
            final_error_code=15,
            intermediate_error_codes=[],
            status=TransferStatus.INTEGRATED,
            date_requested=date_requested,
            date_completed=date_completed,
        )
    ]

    assert list(actual) == expected


def test_excludes_failed_transfers():
    integrated_transfer_1 = build_transfer(status=TransferStatus.INTEGRATED)
    integrated_transfer_2 = build_transfer(status=TransferStatus.INTEGRATED)
    failed_transfer = build_transfer(status=TransferStatus.FAILED)
    transfers = [integrated_transfer_1, integrated_transfer_2, failed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [integrated_transfer_1, integrated_transfer_2]

    assert list(actual) == expected


def test_excludes_transfers_missing_sla_duration():
    integrated_transfer_1 = build_transfer(status=TransferStatus.INTEGRATED)
    integrated_transfer_2 = build_transfer(status=TransferStatus.INTEGRATED, sla_duration=None)
    failed_transfer = build_transfer(status=TransferStatus.FAILED)
    transfers = [integrated_transfer_1, integrated_transfer_2, failed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [integrated_transfer_1]

    assert list(actual) == expected


def test_excludes_pending_transfers():
    pending_transfer = build_transfer(status=TransferStatus.PENDING)

    transfers = [pending_transfer]

    actual = filter_for_successful_transfers(transfers)
    expected = []

    assert list(actual) == expected
