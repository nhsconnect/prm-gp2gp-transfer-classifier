def calculate_percentage(portion: int, total: int, num_digits: int):
    if total == 0:
        return None
    return round((portion / total) * 100, num_digits)
