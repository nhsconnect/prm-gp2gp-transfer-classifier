from datetime import datetime, timedelta
from unittest.mock import Mock

from prmdata.pipeline.s3_uri_resolver import TransferClassifierS3UriResolver
from tests.builders.common import a_datetime, a_string


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
        gp2gp_spine_bucket=spine_bucket, transfers_bucket=a_string(), ods_metadata_bucket=a_string()
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


def test_returns_correct_ods_metadata_uris():
    ods_metadata_bucket = a_string()
    reporting_window_dates = [
        datetime(year=2020, month=12, day=31),
        datetime(year=2021, month=1, day=1),
        datetime(year=2021, month=1, day=2),
    ]

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=a_string(),
        ods_metadata_bucket=ods_metadata_bucket,
    )

    expected = [
        f"s3://{ods_metadata_bucket}/v5/2020/12/organisationMetadata.json",
        f"s3://{ods_metadata_bucket}/v5/2021/1/organisationMetadata.json",
    ]

    actual = uri_resolver.ods_metadata(reporting_window_dates)

    assert actual == expected


def test_returns_correct_transfers_uri():
    transfers_bucket = a_string()
    daily_start_datetime = a_datetime(year=2021, month=1, day=3)
    cutoff_number_of_days = 2
    conversation_cutoff = timedelta(days=cutoff_number_of_days)

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=transfers_bucket,
        ods_metadata_bucket=a_string(),
    )

    expected_filename = "2021-01-03-transfers.parquet"
    expected = (
        f"s3://{transfers_bucket}/v11/cutoff-{cutoff_number_of_days}/2021/01/03/{expected_filename}"
    )

    actual = uri_resolver.gp2gp_transfers(daily_start_datetime, cutoff=conversation_cutoff)

    assert actual == expected


def test_returns_correct_ods_uri_metadata_for_previous_month_when_missing_metadata():
    ods_metadata_bucket = a_string()
    reporting_window_dates = [
        datetime(year=2021, month=1, day=1),
    ]

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=a_string(),
        ods_metadata_bucket=ods_metadata_bucket,
    )

    expected = [
        f"s3://{ods_metadata_bucket}/v5/2020/12/organisationMetadata.json",
    ]

    actual = uri_resolver.ods_metadata_using_previous_month(reporting_window_dates)

    assert actual == expected


def test_returns_correct_ods_uris_metadata_for_previous_months_when_missing_metadata():
    ods_metadata_bucket = a_string()
    reporting_window_dates = [
        datetime(year=2020, month=12, day=31),
        datetime(year=2021, month=1, day=1),
        datetime(year=2021, month=2, day=1),
        datetime(year=2021, month=3, day=1),
    ]

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=a_string(),
        ods_metadata_bucket=ods_metadata_bucket,
    )

    expected = [
        f"s3://{ods_metadata_bucket}/v5/2020/12/organisationMetadata.json",
        f"s3://{ods_metadata_bucket}/v5/2021/1/organisationMetadata.json",
        f"s3://{ods_metadata_bucket}/v5/2021/2/organisationMetadata.json",
    ]

    actual = uri_resolver.ods_metadata_using_previous_month(reporting_window_dates)

    assert actual == expected


def test_returns_correct_mi_event_uris():
    mi_bucket = a_string()

    reporting_window = Mock()
    reporting_window.get_dates = Mock(
        return_value=[
            datetime(year=2020, month=12, day=1),
            datetime(year=2020, month=12, day=2),
        ]
    )
    reporting_window.get_overflow_dates = Mock(
        return_value=[
            datetime(year=2021, month=1, day=1),
            datetime(year=2021, month=1, day=2),
        ]
    )

    uri_resolver = TransferClassifierS3UriResolver(
        gp2gp_spine_bucket=a_string(),
        transfers_bucket=a_string(),
        ods_metadata_bucket=a_string(),
        mi_bucket=mi_bucket,
    )

    expected = [
        f"s3://{mi_bucket}/v1/2020/12/01",
        f"s3://{mi_bucket}/v1/2020/12/02",
        f"s3://{mi_bucket}/v1/2021/01/01",
        f"s3://{mi_bucket}/v1/2021/01/02",
    ]

    actual = uri_resolver.mi_events(reporting_window)

    assert actual == expected
