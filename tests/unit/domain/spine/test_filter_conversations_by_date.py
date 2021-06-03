from datetime import datetime

from prmdata.domain.spine.parsed_conversation import filter_conversations_by_request_started_time
from prmdata.utils.reporting_window import MonthlyReportingWindow
from tests.builders.spine import build_parsed_conversation, build_message


def test_filter_conversations_by_request_started_time_keeps_conversation_within_range():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(year=2020, month=6, day=1),
        overflow_month_start=datetime(year=2020, month=7, day=1),
    )

    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=6, day=6))
        )
    ]

    expected = parsed_conversations

    actual = filter_conversations_by_request_started_time(parsed_conversations, reporting_window)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversation_before_range():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(year=2020, month=6, day=1),
        overflow_month_start=datetime(year=2020, month=7, day=1),
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=5, day=28))
        )
    ]

    expected = []

    actual = filter_conversations_by_request_started_time(parsed_conversations, reporting_window)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversation_after_range():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(year=2020, month=6, day=1),
        overflow_month_start=datetime(year=2020, month=7, day=1),
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=7, day=28))
        )
    ]

    expected = []

    actual = filter_conversations_by_request_started_time(parsed_conversations, reporting_window)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversations_outside_of_range():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(year=2020, month=6, day=1),
        overflow_month_start=datetime(year=2020, month=7, day=1),
    )

    conversation_within_range = build_parsed_conversation(
        request_started=build_message(time=datetime(year=2020, month=6, day=15))
    )
    conversation_before_range = build_parsed_conversation(
        request_started=build_message(time=datetime(year=2020, month=5, day=28))
    )
    conversation_after_range = build_parsed_conversation(
        request_started=build_message(time=datetime(year=2020, month=7, day=28))
    )

    parsed_conversations = [
        conversation_before_range,
        conversation_within_range,
        conversation_after_range,
    ]

    expected = [conversation_within_range]

    actual = filter_conversations_by_request_started_time(parsed_conversations, reporting_window)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_accepts_conversation_on_range_start():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(year=2020, month=6, day=1),
        overflow_month_start=datetime(year=2020, month=7, day=1),
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=6, day=1))
        )
    ]

    expected = parsed_conversations

    actual = filter_conversations_by_request_started_time(parsed_conversations, reporting_window)

    assert list(actual) == expected


def test_filter_conversations_by_request_started_time_rejects_conversation_on_range_end():
    reporting_window = MonthlyReportingWindow(
        metric_month_start=datetime(year=2020, month=6, day=1),
        overflow_month_start=datetime(year=2020, month=7, day=1),
    )
    parsed_conversations = [
        build_parsed_conversation(
            request_started=build_message(time=datetime(year=2020, month=7, day=1))
        )
    ]

    expected = []

    actual = filter_conversations_by_request_started_time(parsed_conversations, reporting_window)

    assert list(actual) == expected
