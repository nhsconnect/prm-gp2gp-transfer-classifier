from datetime import datetime, timedelta
from unittest.mock import Mock

from prmdata.pipeline.io import (
    TransferClassifierMonthlyS3UriResolver,
    TransferClassifierS3UriResolver,
)
from tests.builders.common import a_datetime, a_string


def test_returns_correct_monthly_spine_messages_uris():
    spine_bucket = a_string()

    metric_month = (2021, 1)
    overflow_month = (2021, 2)

    uri_resolver = TransferClassifierMonthlyS3UriResolver(
        gp2gp_spine_bucket=spine_bucket,
        transfers_bucket=a_string(),
    )

    expected = [
        f"s3://{spine_bucket}/v2/messages/2021/1/2021-1_spine_messages.csv.gz",
        f"s3://{spine_bucket}/v2/messages-overflow/2021/2/2021-2_spine_messages_overflow.csv.gz",
    ]

    actual = uri_resolver.spine_messages(metric_month, overflow_month)

    assert actual == expected


def test_returns_correct_monthly_transfers_uri():
    transfers_bucket = a_string()
    year_month = (2021, 1)

    uri_resolver = TransferClassifierMonthlyS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=transfers_bucket,
    )

    expected = f"s3://{transfers_bucket}/v6/2021/1/2021-1-transfers.parquet"

    actual = uri_resolver.gp2gp_transfers(year_month)

    assert actual == expected


def test_returns_correct_spine_messages_uris():
    spine_bucket = a_string()
    reporting_window = Mock()
    reporting_window.get_dates = Mock(
        return_value=[
            datetime(year=2020, month=12, day=30),
            datetime(year=2020, month=12, day=31),
            datetime(year=2021, month=1, day=1),
        ]
    )
    reporting_window.get_overflow_dates = Mock(
        return_value=[
            datetime(year=2021, month=1, day=2),
            datetime(year=2021, month=1, day=3),
        ]
    )

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=spine_bucket,
        transfers_bucket=a_string(),
    )

    expected = [
        f"s3://{spine_bucket}/v3/2020/12/30/2020-12-30_spine_messages.csv.gz",
        f"s3://{spine_bucket}/v3/2020/12/31/2020-12-31_spine_messages.csv.gz",
        f"s3://{spine_bucket}/v3/2021/01/01/2021-01-01_spine_messages.csv.gz",
        f"s3://{spine_bucket}/v3/2021/01/02/2021-01-02_spine_messages.csv.gz",
        f"s3://{spine_bucket}/v3/2021/01/03/2021-01-03_spine_messages.csv.gz",
    ]

    actual = uri_resolver.spine_messages(reporting_window)

    assert actual == expected


def test_returns_correct_transfers_uri():
    transfers_bucket = a_string()
    daily_start_datetime = a_datetime(year=2021, month=1, day=3)
    cutoff_number_of_days = 2
    conversation_cutoff = timedelta(days=cutoff_number_of_days)

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=transfers_bucket,
    )

    expected_filename = "2021-01-03-transfers.parquet"
    expected = (
        f"s3://{transfers_bucket}/v7/cutoff-{cutoff_number_of_days}/2021/01/03/{expected_filename}"
    )

    actual = uri_resolver.gp2gp_transfers(daily_start_datetime, cutoff=conversation_cutoff)

    assert actual == expected
