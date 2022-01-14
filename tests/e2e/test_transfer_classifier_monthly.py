import json
import logging
from datetime import datetime
from io import BytesIO
from os import environ
from threading import Thread

import boto3
from botocore.config import Config
from moto.server import DomainDispatcherApplication, create_backend_app
from werkzeug.serving import make_server

from prmdata.pipeline.main import main
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


def test_end_to_end_with_fake_s3(datadir):
    fake_s3_host = "127.0.0.1"
    fake_s3_port = 8886
    fake_s3_url = f"http://{fake_s3_host}:{fake_s3_port}"
    fake_s3_access_key = "testing"
    fake_s3_secret_key = "testing"
    fake_s3_region = "us-west-1"
    s3_output_transfer_data_bucket_name = "output-transfer-data-bucket"
    s3_input_spine_data_bucket_name = "input-spine-data-bucket"

    fake_s3 = _build_fake_s3(fake_s3_host, fake_s3_port)
    fake_s3.start()

    environ["AWS_ACCESS_KEY_ID"] = fake_s3_access_key
    environ["AWS_SECRET_ACCESS_KEY"] = fake_s3_secret_key
    environ["AWS_DEFAULT_REGION"] = fake_s3_region

    environ["INPUT_SPINE_DATA_BUCKET"] = s3_input_spine_data_bucket_name
    environ["OUTPUT_TRANSFER_DATA_BUCKET"] = s3_output_transfer_data_bucket_name
    environ["DATE_ANCHOR"] = "2020-01-30T18:44:49Z"
    environ["CONVERSATION_CUTOFF_DAYS"] = "14"
    environ["S3_ENDPOINT_URL"] = fake_s3_url
    environ["BUILD_TAG"] = "abc456"

    s3 = boto3.resource(
        "s3",
        endpoint_url=fake_s3_url,
        aws_access_key_id=fake_s3_access_key,
        aws_secret_access_key=fake_s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=fake_s3_region,
    )

    output_transfer_data_bucket = _build_fake_s3_bucket(s3_output_transfer_data_bucket_name, s3)
    input_spine_data_bucket = _build_fake_s3_bucket(s3_input_spine_data_bucket_name, s3)

    expected_transfers_output_key = "transfers.parquet"

    expected_metadata = {
        "date-anchor": "2020-01-30T18:44:49+00:00",
        "cutoff-days": "14",
        "build-tag": "abc456",
    }

    expected_transfers = _read_parquet_columns_json(
        datadir / "expected_outputs" / "transfersParquetColumns.json"
    )

    input_csv_gz = read_file_to_gzip_buffer(datadir / "inputs" / "Dec-2019.csv")
    input_spine_data_bucket.upload_fileobj(
        input_csv_gz, "v2/messages/2019/12/2019-12_spine_messages.csv.gz"
    )

    input_overflow_csv_gz = read_file_to_gzip_buffer(datadir / "inputs" / "Jan-2020-overflow.csv")
    input_spine_data_bucket.upload_fileobj(
        input_overflow_csv_gz, "v2/messages-overflow/2020/1/2020-1_spine_messages_overflow.csv.gz"
    )

    s3_output_path = f"v6/2019/12/2019-12-{expected_transfers_output_key}"

    try:
        main()
        actual_transfers = read_s3_parquet(output_transfer_data_bucket, s3_output_path)
        actual_metadata = _read_s3_metadata(output_transfer_data_bucket, s3_output_path)

        assert actual_transfers == expected_transfers
        assert actual_metadata == expected_metadata

    finally:
        output_transfer_data_bucket.objects.all().delete()
        output_transfer_data_bucket.delete()
        fake_s3.stop()
        environ.clear()
