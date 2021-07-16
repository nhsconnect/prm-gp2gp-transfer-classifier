from typing import List

from prmdata.domain.gp2gp.transfer import (
    Transfer,
    TransferStatus,
    filter_for_successful_transfers,
    TransferOutcome,
    TransferFailureReason,
)
from tests.builders.gp2gp import (
    build_transfer,
    an_integrated_transfer,
    a_supressed_transfer,
    a_transfer_with_a_final_error,
    a_transfer_where_no_core_ehr_was_sent,
    a_transfer_integrated_beyond_8_days,
)
from tests.builders.common import a_datetime

date_requested = a_datetime()
date_completed = a_datetime()


def test_includes_successful_transfer():

    transfers = [an_integrated_transfer()]

    actual = filter_for_successful_transfers(transfers)

    expected = transfers

    assert list(actual) == expected


def test_includes_suppressed_transfers():
    transfers = [a_supressed_transfer()]

    actual = filter_for_successful_transfers(transfers)

    expected = transfers

    assert list(actual) == expected


def test_includes_late_integrations():
    transfers = [a_transfer_integrated_beyond_8_days()]

    actual = filter_for_successful_transfers(transfers)
    expected: List[Transfer] = transfers

    assert list(actual) == expected


def test_excludes_failed_transfers():
    integrated_transfer_1 = an_integrated_transfer()
    integrated_transfer_2 = an_integrated_transfer()
    failed_transfer = a_transfer_with_a_final_error()
    transfers = [integrated_transfer_1, integrated_transfer_2, failed_transfer]

    actual = filter_for_successful_transfers(transfers)

    expected = [integrated_transfer_1, integrated_transfer_2]

    assert list(actual) == expected


def test_excludes_transfers_missing_sla_duration():
    integrated_transfer_1 = an_integrated_transfer()
    integrated_transfer_2 = build_transfer(
        transfer_outcome=TransferOutcome(
            status=TransferStatus.INTEGRATED_ON_TIME, reason=TransferFailureReason.DEFAULT
        ),
        sla_duration=None,
    )
    transfers = [integrated_transfer_1, integrated_transfer_2]

    actual = filter_for_successful_transfers(transfers)

    expected = [integrated_transfer_1]

    assert list(actual) == expected


def test_excludes_pending_transfers():
    transfers = [a_transfer_where_no_core_ehr_was_sent()]

    actual = filter_for_successful_transfers(transfers)
    expected: List[Transfer] = []

    assert list(actual) == expected
