from prmdata.gp2gp.transformers import filter_failed_transfers
from tests.builders.gp2gp import build_transfer


def test_filter_failed_transfers_excludes_failed():
    failed_transfer = build_transfer(error_code=99)
    transfers = [failed_transfer]

    actual = filter_failed_transfers(transfers)

    expected = []

    assert list(actual) == expected


def test_filter_failed_transfers_doesnt_exclude_suppressions():
    suppressed_transfer = build_transfer(error_code=15)
    transfers = [suppressed_transfer]

    actual = filter_failed_transfers(transfers)

    expected = [suppressed_transfer]

    assert list(actual) == expected
