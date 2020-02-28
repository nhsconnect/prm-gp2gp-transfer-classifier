from datetime import timedelta

from gp2gp.service.models import SuccessfulTransfer
from gp2gp.service.transformers import filter_for_successful_transfers
from tests.builders.service import build_transfer


def test_includes_successful_transfer():
    successful_transfer = build_transfer(
        conversation_id="123",
        sla_duration=timedelta(hours=1),
        requesting_practice_ods_code="A12345",
        sending_practice_ods_code="B67890",
        pending=False,
        error_code=None,
    )

    transfers = [successful_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [
        SuccessfulTransfer(
            conversation_id="123",
            sla_duration=timedelta(hours=1),
            requesting_practice_ods_code="A12345",
            sending_practice_ods_code="B67890",
        )
    ]

    assert list(actual) == expected


def test_includes_suppressed_transfers():
    suppressed_transfer = build_transfer(
        conversation_id="456",
        sla_duration=timedelta(hours=2),
        requesting_practice_ods_code="B12345",
        sending_practice_ods_code="A67890",
        pending=False,
        error_code=15,
    )

    transfers = [suppressed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [
        SuccessfulTransfer(
            conversation_id="456",
            sla_duration=timedelta(hours=2),
            requesting_practice_ods_code="B12345",
            sending_practice_ods_code="A67890",
        )
    ]

    assert list(actual) == expected


def test_includes_successful_transfers():
    transfers = [build_transfer(pending=False, error_code=None) for _ in range(3)]

    actual = filter_for_successful_transfers(transfers)

    expected_length = 3

    assert len(list(actual)) == expected_length


def test_excludes_failed_transfers():
    failed_transfer = build_transfer(error_code=99)
    transfers = [failed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = []

    assert list(actual) == expected


def test_excludes_pending_transfers():
    pending_transfer = build_transfer(pending=True)

    transfers = [pending_transfer]

    actual = filter_for_successful_transfers(transfers)
    expected = []

    assert list(actual) == expected
