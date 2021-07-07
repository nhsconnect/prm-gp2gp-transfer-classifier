from datetime import datetime
import json
import logging
from io import BytesIO
from os import environ
from threading import Thread

import boto3
from botocore.config import Config
from moto.server import DomainDispatcherApplication, create_backend_app
from prmdata.pipeline.platform_metrics_calculator.main import main
from werkzeug.serving import make_server

from tests.builders.file import read_file_to_gzip_buffer
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

EXPECTED_TRANSFERS = {
    "conversation_id": [
        "integrated-within-8-days--A12345",
        "integrated-beyond-8-days--A12345",
        "completed-within-3-days-in-jan--A12345",
        "failed--A12345",
        "integrated-within-8-days--A12347",
        "completed-within-3-days--A12347",
    ],
    "date_completed": [
        datetime(2019, 12, 6, 8, 41, 48, 337000),
        datetime(2019, 12, 15, 8, 41, 48, 337000),
        datetime(2020, 1, 1, 8, 41, 48, 337000),
        datetime(2019, 12, 20, 8, 41, 48, 337000),
        datetime(2019, 12, 7, 8, 41, 48, 337000),
        datetime(2019, 12, 31, 18, 3, 24, 982000),
    ],
    "date_requested": [
        datetime(2019, 12, 1, 18, 2, 29, 985000),
        datetime(2019, 12, 5, 18, 2, 29, 985000),
        datetime(2019, 12, 30, 18, 2, 29, 985000),
        datetime(2019, 12, 19, 18, 2, 29, 985000),
        datetime(2019, 12, 3, 18, 2, 29, 985000),
        datetime(2019, 12, 31, 18, 2, 29, 985000),
    ],
    "final_error_codes": [[None], [None], [None], [30], [None], [None]],
    "intermediate_error_codes": [[], [], [], [], [], []],
    "requesting_practice_asid": [
        "123456789123",
        "123456789123",
        "123456789123",
        "123456789123",
        "987654321240",
        "987654321240",
    ],
    "requesting_supplier": ["", "SystmOne", "SystmOne", "Vision", "", "SystmOne"],
    "sender_error_code": [None, None, None, None, None, None],
    "sending_practice_asid": [
        "003456789123",
        "003456789123",
        "003456789123",
        "003456789123",
        "003456789123",
        "003456789123",
    ],
    "sending_supplier": ["", "EMIS", "Vision", "Unknown", "", "Vision"],
    "sla_duration": [398306, 830306, 139106, 52706, 311906, 3],
    "status": [
        "INTEGRATED_ON_TIME",
        "PROCESS_FAILURE",
        "INTEGRATED_ON_TIME",
        "TECHNICAL_FAILURE",
        "INTEGRATED_ON_TIME",
        "INTEGRATED_ON_TIME",
    ],
    "failure_reason": ["", "Integrated Late", "", "Final Error", "", ""],
}


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


def _read_s3_json(bucket, key):
    f = BytesIO()
    bucket.download_fileobj(key, f)
    f.seek(0)
    return json.loads(f.read().decode("utf-8"))


def _read_s3_parquet(bucket, key):
    f = BytesIO()
    bucket.download_fileobj(key, f)
    return pq.read_table(f).to_pydict()


def _build_fake_s3(host, port):
    app = DomainDispatcherApplication(create_backend_app, "s3")
    server = make_server(host, port, app)
    return ThreadedServer(server)


def _build_fake_s3_bucket(bucket_name: str, s3):
    s3_fake_bucket = s3.Bucket(bucket_name)
    s3_fake_bucket.create()
    return s3_fake_bucket


def test_with_s3_output(datadir):
    fake_s3_host = "127.0.0.1"
    fake_s3_port = 8887
    fake_s3_url = f"http://{fake_s3_host}:{fake_s3_port}"
    fake_s3_access_key = "testing"
    fake_s3_secret_key = "testing"
    fake_s3_region = "us-west-1"
    s3_output_transfer_data_bucket_name = "output-transfer-data-bucket"
    s3_input_transfer_data_bucket_name = "input-transfer-data-bucket"
    s3_organisation_metadata_bucket_name = "organisation-metadata-bucket"

    fake_s3 = _build_fake_s3(fake_s3_host, fake_s3_port)
    fake_s3.start()

    environ["AWS_ACCESS_KEY_ID"] = fake_s3_access_key
    environ["AWS_SECRET_ACCESS_KEY"] = fake_s3_secret_key
    environ["AWS_DEFAULT_REGION"] = fake_s3_region

    environ["INPUT_TRANSFER_DATA_BUCKET"] = s3_input_transfer_data_bucket_name
    environ["OUTPUT_TRANSFER_DATA_BUCKET"] = s3_output_transfer_data_bucket_name
    environ["ORGANISATION_METADATA_BUCKET"] = s3_organisation_metadata_bucket_name
    environ["DATE_ANCHOR"] = "2020-01-30T18:44:49Z"
    environ["S3_ENDPOINT_URL"] = fake_s3_url

    s3 = boto3.resource(
        "s3",
        endpoint_url=fake_s3_url,
        aws_access_key_id=fake_s3_access_key,
        aws_secret_access_key=fake_s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=fake_s3_region,
    )

    output_transfer_data_bucket = _build_fake_s3_bucket(s3_output_transfer_data_bucket_name, s3)
    input_transfer_data_bucket = _build_fake_s3_bucket(s3_input_transfer_data_bucket_name, s3)
    organisation_metadata_bucket = _build_fake_s3_bucket(s3_organisation_metadata_bucket_name, s3)

    expected_practice_metrics_output_key = "practiceMetrics.json"
    expected_national_metrics_output_key = "nationalMetrics.json"
    expected_transfers_output_key = "transfers.parquet"

    expected_practice_metrics = _read_json(datadir / "expected_outputs" / "practiceMetrics.json")
    expected_national_metrics = _read_json(datadir / "expected_outputs" / "nationalMetrics.json")

    input_csv_gz = read_file_to_gzip_buffer(datadir / "inputs" / "Dec-2019.csv")
    input_transfer_data_bucket.upload_fileobj(
        input_csv_gz, "v2/messages/2019/12/2019-12_spine_messages.csv.gz"
    )

    input_overflow_csv_gz = read_file_to_gzip_buffer(datadir / "inputs" / "Jan-2020-overflow.csv")
    input_transfer_data_bucket.upload_fileobj(
        input_overflow_csv_gz, "v2/messages-overflow/2020/1/2020-1_spine_messages_overflow.csv.gz"
    )

    organisation_metadata_file = str(datadir / "inputs" / "organisationMetadata.json")
    organisation_metadata_bucket.upload_file(
        organisation_metadata_file, "v2/2020/1/organisationMetadata.json"
    )

    s3_output_path = "v3/2019/12/"

    try:
        main()
        actual_practice_metrics = _read_s3_json(
            output_transfer_data_bucket, f"{s3_output_path}{expected_practice_metrics_output_key}"
        )
        actual_national_metrics = _read_s3_json(
            output_transfer_data_bucket, f"{s3_output_path}{expected_national_metrics_output_key}"
        )
        actual_transfers = _read_s3_parquet(
            output_transfer_data_bucket, f"{s3_output_path}{expected_transfers_output_key}"
        )

        assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
        assert actual_practice_metrics["ccgs"] == expected_practice_metrics["ccgs"]
        assert actual_national_metrics["metrics"] == expected_national_metrics["metrics"]

        assert actual_transfers == EXPECTED_TRANSFERS
    finally:
        output_transfer_data_bucket.objects.all().delete()
        output_transfer_data_bucket.delete()
        fake_s3.stop()
