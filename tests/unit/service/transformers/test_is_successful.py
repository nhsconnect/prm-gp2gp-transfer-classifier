from gp2gp.service.transformers import is_successful
from tests.builders.service import build_transfer


def test_returns_false_given_failed_transfer():
    failed_transfer = build_transfer(error_code=99)

    actual = is_successful(failed_transfer)

    expected = False

    assert actual == expected


def test_returns_true_given_suppressed_transfer():
    suppressed_transfer = build_transfer(error_code=15)

    actual = is_successful(suppressed_transfer)

    expected = True

    assert actual == expected


def test_returns_true_given_successful_transfer():
    successful_transfer = build_transfer(error_code=None)

    actual = is_successful(successful_transfer)

    expected = True

    assert actual == expected
