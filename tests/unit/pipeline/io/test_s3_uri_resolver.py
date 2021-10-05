from prmdata.domain.datetime import MonthlyReportingWindow
from prmdata.pipeline.io import TransferClassifierS3UriResolver
from tests.builders.common import a_string, a_datetime


def test_returns_correct_spine_messages_uris():
    spine_bucket = a_string()
    date_anchor = a_datetime(year=2021, month=2)
    reporting_window = MonthlyReportingWindow.prior_to(date_anchor)

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=spine_bucket,
        transfers_bucket=a_string(),
    )

    expected = [
        f"s3://{spine_bucket}/v2/messages/2021/1/2021-1_spine_messages.csv.gz",
        f"s3://{spine_bucket}/v2/messages-overflow/2021/2/2021-2_spine_messages_overflow.csv.gz",
    ]

    actual = uri_resolver.spine_messages(reporting_window)

    assert actual == expected


def test_returns_correct_transfers_uri():
    transfers_bucket = a_string()
    date_anchor = a_datetime(year=2021, month=2)
    reporting_window = MonthlyReportingWindow.prior_to(date_anchor)

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=transfers_bucket,
    )

    expected = f"s3://{transfers_bucket}/v5/2021/1/2021-1-transfers.parquet"

    actual = uri_resolver.gp2gp_transfers(reporting_window)

    assert actual == expected
