from prmdata.utils.calculate_percentage import calculate_percentage


def test_rounds_down_1_digit():
    expected = 33.3
    actual = calculate_percentage(portion=1, total=3, num_digits=1)

    assert actual == expected


def test_rounds_up_2_digits():
    expected = 66.67
    actual = calculate_percentage(portion=2, total=3, num_digits=2)

    assert actual == expected
