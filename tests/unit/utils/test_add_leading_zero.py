import pytest

from prmdata.utils.add_leading_zero import add_leading_zero


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (0, "00"),
        (1, "01"),
        (12, "12"),
        (123, "123"),
    ],
)
def test_leading_zero_adder_returns_string_with_leading_zero(test_input, expected):
    actual = add_leading_zero(test_input)
    assert actual == expected
