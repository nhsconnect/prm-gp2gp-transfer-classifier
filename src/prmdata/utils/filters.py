def find_first(iterable, value=lambda x: x, default=None, condition=lambda x: True):
    """
    Returns the first item in the `iterable` that
    satisfies the `condition`.

    If the condition is not given, returns the first item of
    the iterable.

    If the `default` argument is given and the iterable is empty,
    or if it has no items matching the condition, the `default` argument
    is returned if it matches the condition.

    The `default` argument being None is the same as it not being given.

    >>> find_first( (1,2,3), condition=lambda x: x % 2 == 0)
    2
    >>> find_first(range(3, 100))
    3


    If a specific value from an object needs to be returned, it can be specified as a lambda using the value field.
    >>> an_iterable = ({"key": "value1", "other": "value_1b"},{"key": "value2", "other": "value_2b"})
    >>> find_first( iterable=an_iterable, value=lambda x: x.get("other"), condition=lambda x: x.get("key") == "value1" )
    "value_1b"
    """

    try:
        return next(value(x) for x in iterable if condition(x))
    except StopIteration:
        return default
