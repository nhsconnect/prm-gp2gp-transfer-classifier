import json
import logging
from datetime import datetime, timedelta
from io import BytesIO
from os import environ
from threading import Thread

import boto3
from botocore.config import Config
from dateutil.tz import UTC
from freezegun import freeze_time
from moto.server import DomainDispatcherApplication, create_backend_app
from werkzeug.serving import make_server

from prmdata.pipeline.main import main
from prmdata.utils.add_leading_zero import add_leading_zero
from tests.builders.file import read_file_to_gzip_buffer
from tests.builders.s3 import read_s3_parquet

logger = logging.getLogger(__name__)


class ThreadedServer:
    def __init__(self, server):
        self._server = server
        self._thread = Thread(target=server.serve_forever)

    def start(self):
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        self._thread.join()


FAKE_AWS_HOST = "127.0.0.1"
FAKE_AWS_PORT = 8887
FAKE_AWS_URL = f"http://{FAKE_AWS_HOST}:{FAKE_AWS_PORT}"
FAKE_S3_ACCESS_KEY = "testing"
FAKE_S3_SECRET_KEY = "testing"
FAKE_S3_REGION = "us-west-1"

S3_INPUT_SPINE_DATA_BUCKET_NAME = "input-spine-data-bucket"
S3_INPUT_ODS_METADATA_BUCKET_NAME = "input-ods-metadata-bucket"
S3_OUTPUT_TRANSFER_DATA_BUCKET_NAME = "output-transfer-data-bucket"


def _setup():
    s3_client = boto3.resource(
        "s3",
        endpoint_url=FAKE_AWS_URL,
        aws_access_key_id=FAKE_S3_ACCESS_KEY,
        aws_secret_access_key=FAKE_S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=FAKE_S3_REGION,
    )

    environ["AWS_ACCESS_KEY_ID"] = FAKE_S3_ACCESS_KEY
    environ["AWS_SECRET_ACCESS_KEY"] = FAKE_S3_SECRET_KEY
    environ["AWS_DEFAULT_REGION"] = FAKE_S3_REGION

    environ["INPUT_SPINE_DATA_BUCKET"] = S3_INPUT_SPINE_DATA_BUCKET_NAME
    environ["INPUT_ODS_METADATA_BUCKET"] = S3_INPUT_ODS_METADATA_BUCKET_NAME
    environ["OUTPUT_TRANSFER_DATA_BUCKET"] = S3_OUTPUT_TRANSFER_DATA_BUCKET_NAME

    environ["S3_ENDPOINT_URL"] = FAKE_AWS_URL
    environ["BUILD_TAG"] = "abc456"

    fake_s3 = _build_fake_s3(FAKE_AWS_HOST, FAKE_AWS_PORT)
    return fake_s3, s3_client


def _read_json(path):
    return json.loads(path.read_text())


def _parse_dates(items):
    return [None if item is None else datetime.fromisoformat(item) for item in items]


def _read_parquet_columns_json(path):
    columns_with_dates = ["date_requested", "date_completed", "last_sender_message_timestamp"]

    return {
        column_name: _parse_dates(values) if column_name in columns_with_dates else values
        for column_name, values in _read_json(path).items()
    }


def _read_s3_metadata(bucket, key):
    return bucket.Object(key).get()["Metadata"]


def _read_s3_json(bucket, key):
    f = BytesIO()
    bucket.download_fileobj(key, f)
    f.seek(0)
    return json.loads(f.read().decode("utf-8"))


def _build_fake_s3(host, port):
    app = DomainDispatcherApplication(create_backend_app, "s3")
    server = make_server(host, port, app)
    return ThreadedServer(server)


def _build_fake_s3_bucket(bucket_name: str, s3):
    s3_fake_bucket = s3.Bucket(bucket_name)
    s3_fake_bucket.create()
    return s3_fake_bucket


def _upload_files_to_ods_metadata_bucket(input_ods_metadata_bucket, datadir):
    organisation_metadata_file = str(datadir / "inputs" / "organisationMetadata.json")
    input_ods_metadata_bucket.upload_file(
        organisation_metadata_file, "v2/2019/12/organisationMetadata.json"
    )


def _upload_files_to_spine_data_bucket(input_spine_data_bucket, datadir):

    _upload_template_spine_data(
        datadir, input_spine_data_bucket, year=2019, data_month=12, time_range=range(1, 32)
    )

    for day in [1, 2, 3, 5, 6, 7, 15, 19, 20, 30, 31]:
        _override_day_spine_messages(
            datadir, input_spine_data_bucket, year=2019, data_month=12, data_day=day
        )

    _upload_template_spine_data(
        datadir, input_spine_data_bucket, year=2020, data_month=1, time_range=range(1, 15)
    )
    _override_day_spine_messages(
        datadir, input_spine_data_bucket, year=2020, data_month=1, data_day=1
    )


def _get_s3_path(year, month, day):
    return f"v3/{year}/{month}/{day}/{year}-{month}-{day}_spine_messages.csv.gz"


def _override_day_spine_messages(
    datadir, input_spine_data_bucket, year: int, data_month: int, data_day: int
):
    day = add_leading_zero(data_day)
    month = add_leading_zero(data_month)

    input_csv_gz = read_file_to_gzip_buffer(
        datadir / "inputs" / f"{year}-{month}-{day}-spine_messages.csv"
    )
    input_spine_data_bucket.upload_fileobj(
        input_csv_gz,
        _get_s3_path(year, month, day),
    )


def _upload_template_spine_data(
    datadir, input_spine_data_bucket, year: int, data_month: int, time_range: range
):
    for data_day in time_range:
        empty_spine_messages = read_file_to_gzip_buffer(
            datadir / "inputs" / "template-spine_messages.csv"
        )
        day = add_leading_zero(data_day)
        month = add_leading_zero(data_month)

        input_spine_data_bucket.upload_fileobj(empty_spine_messages, _get_s3_path(year, month, day))


def _end_datetime_metadata(year: int, data_month: int, data_day: int) -> str:
    end_datetime = datetime(year, data_month, data_day) + timedelta(days=1)
    end_datetime_year = add_leading_zero(end_datetime.year)
    end_datetime_month = add_leading_zero(end_datetime.month)
    end_datetime_day = add_leading_zero(end_datetime.day)
    return f"{end_datetime_year}-{end_datetime_month}-{end_datetime_day}T00:00:00+00:00"


def test_uploads_classified_transfers_given_start_and_end_datetime_and_cutoff(datadir):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_transfer_data_bucket = _build_fake_s3_bucket(
        S3_OUTPUT_TRANSFER_DATA_BUCKET_NAME, s3_client
    )
    input_spine_data_bucket = _build_fake_s3_bucket(S3_INPUT_SPINE_DATA_BUCKET_NAME, s3_client)
    input_ods_metadata_bucket = _build_fake_s3_bucket(S3_INPUT_ODS_METADATA_BUCKET_NAME, s3_client)

    _upload_files_to_spine_data_bucket(input_spine_data_bucket, datadir)
    _upload_files_to_ods_metadata_bucket(input_ods_metadata_bucket, datadir)

    try:
        environ["START_DATETIME"] = "2019-12-02T00:00:00Z"
        environ["END_DATETIME"] = "2020-01-01T00:00:00Z"
        environ["CONVERSATION_CUTOFF_DAYS"] = "14"

        main()

        days_with_data = [2, 3, 5, 19, 20, 30, 31]
        expected_days = [(2019, 12, day) for day in range(2, 32)]
        expected_transfers_output_key = "transfers.parquet"

        for (year, data_month, data_day) in expected_days:
            month = add_leading_zero(data_month)
            day = add_leading_zero(data_day)

            if data_day in days_with_data:
                expected_transfers = _read_parquet_columns_json(
                    datadir / "expected_outputs" / f"{year}-{month}-{day}-transferParquet.json"
                )
            else:
                expected_transfers = _read_parquet_columns_json(
                    datadir / "expected_outputs" / "template-transferParquet.json"
                )

            s3_filename = f"{year}-{month}-{day}-{expected_transfers_output_key}"
            s3_output_path = f"v7/cutoff-14/{year}/{month}/{day}/{s3_filename}"

            actual_transfers = read_s3_parquet(output_transfer_data_bucket, s3_output_path)

            assert actual_transfers == expected_transfers

            actual_metadata = _read_s3_metadata(output_transfer_data_bucket, s3_output_path)

            expected_metadata = {
                "cutoff-days": "14",
                "build-tag": "abc456",
                "start-datetime": f"{year}-{month}-{day}T00:00:00+00:00",
                "end-datetime": _end_datetime_metadata(year, data_month, data_day),
            }

            assert actual_metadata == expected_metadata

    finally:
        output_transfer_data_bucket.objects.all().delete()
        output_transfer_data_bucket.delete()
        fake_s3.stop()
        environ.clear()


@freeze_time(datetime(year=2020, month=1, day=1, hour=3, minute=0, second=0, tzinfo=UTC))
def test_uploads_classified_transfers_given__no__start_and_end_datetimes_and_no_cutoff(datadir):
    fake_s3, s3_client = _setup()
    fake_s3.start()

    output_transfer_data_bucket = _build_fake_s3_bucket(
        S3_OUTPUT_TRANSFER_DATA_BUCKET_NAME, s3_client
    )
    input_spine_data_bucket = _build_fake_s3_bucket(S3_INPUT_SPINE_DATA_BUCKET_NAME, s3_client)
    input_ods_metadata_bucket = _build_fake_s3_bucket(S3_INPUT_ODS_METADATA_BUCKET_NAME, s3_client)

    _upload_files_to_spine_data_bucket(input_spine_data_bucket, datadir)
    _upload_files_to_ods_metadata_bucket(input_ods_metadata_bucket, datadir)

    try:
        main()

        expected_transfers_output_key = "transfers.parquet"
        expected_metadata = {
            "cutoff-days": "0",
            "build-tag": "abc456",
            "start-datetime": "2019-12-31T00:00:00+00:00",
            "end-datetime": "2020-01-01T00:00:00+00:00",
        }
        year = 2019
        month = 12
        day = 31

        expected_transfers = _read_parquet_columns_json(
            datadir / "expected_outputs" / f"{year}-{month}-{day}-transferParquet.json"
        )

        s3_filename = f"{year}-{month}-{day}-{expected_transfers_output_key}"
        s3_output_path = f"v7/cutoff-0/{year}/{month}/{day}/{s3_filename}"

        actual_transfers = read_s3_parquet(output_transfer_data_bucket, s3_output_path)

        assert actual_transfers == expected_transfers

        actual_metadata = _read_s3_metadata(output_transfer_data_bucket, s3_output_path)

        assert actual_metadata == expected_metadata

    finally:
        output_transfer_data_bucket.objects.all().delete()
        output_transfer_data_bucket.delete()
        fake_s3.stop()
        environ.clear()
