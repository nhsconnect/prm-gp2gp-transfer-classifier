import csv
from datetime import datetime
import json
import logging
from io import BytesIO
from os import getenv, environ
from threading import Thread

import boto3
from botocore.config import Config
from moto.server import DomainDispatcherApplication, create_backend_app
from prmdata.pipeline.platform_metrics_calculator.config import DataPipelineConfig
from prmdata.pipeline.platform_metrics_calculator.main_new import main
from werkzeug.serving import make_server
from tests.builders.file import gzip_file, build_gzip_csv
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
    "status": ["INTEGRATED", "INTEGRATED", "INTEGRATED", "FAILED", "INTEGRATED", "INTEGRATED"],
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


def _read_parquet(path):
    return pq.read_table(path).to_pydict()


def _gzip_files(file_paths):
    return [gzip_file(file_path) for file_path in file_paths]


def _csv_join(strings):
    return ",".join(strings)


def _csv_join_paths(paths):
    return _csv_join([str(p) for p in paths])


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


###########################################################
###########################################################
###########################################################
def test_with_s3_output(datadir):

    fake_s3_host = "127.0.0.1"
    fake_s3_port = 8887
    fake_s3_url = f"http://{fake_s3_host}:{fake_s3_port}"
    fake_s3_access_key = "testing"
    fake_s3_secret_key = "testing"
    fake_s3_region = "us-west-1"

    fake_s3 = _build_fake_s3(fake_s3_host, fake_s3_port)
    fake_s3.start()

    environ["AWS_ACCESS_KEY_ID"] = fake_s3_access_key
    environ["AWS_SECRET_ACCESS_KEY"] = fake_s3_secret_key
    environ["AWS_DEFAULT_REGION"] = fake_s3_region
    environ["PATH"] = getenv("PATH")

    s3 = boto3.resource(
        "s3",
        endpoint_url=fake_s3_url,
        aws_access_key_id=fake_s3_access_key,
        aws_secret_access_key=fake_s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=fake_s3_region,
    )

    s3_test_bucket = "testbucket"
    output_bucket = s3.Bucket(s3_test_bucket)
    output_bucket.create()

    expected_practice_metrics_output_key = "practiceMetrics.json"
    expected_organisation_metadata_output_key = "organisationMetadata.json"
    expected_national_metrics_output_key = "nationalMetrics.json"
    expected_transfers_output_key = "transfers.parquet"

    expected_practice_metrics = _read_json(
        datadir / "expected_json_output" / "v2" / "2019" / "12" / "practiceMetrics.json"
    )
    expected_organisation_metadata = _read_json(
        datadir / "expected_json_output" / "v2" / "2019" / "12" / "organisationMetadata.json"
    )
    expected_national_metrics = _read_json(
        datadir / "expected_json_output" / "v2" / "2019" / "12" / "nationalMetrics.json"
    )

    s3_path = "v2/2019/12/"

    input_csv = csv.reader(open(datadir / "v2" / "2019" / "12" / "Dec-2019.csv"))
    input_csv = list(input_csv)
    input_csv_gz = BytesIO(build_gzip_csv(header=input_csv[0], rows=input_csv[1:]))
    output_bucket.upload_fileobj(input_csv_gz, "v2/2019/12/Dec-2019.csv.gz")

    input_overflow_csv = csv.reader(
        open(datadir / "v2" / "2020" / "1" / "overflow" / "Jan-2020.csv")
    )
    input_overflow_csv = list(input_overflow_csv)
    input_overflow_csv_gz = BytesIO(
        build_gzip_csv(header=input_overflow_csv[0], rows=input_overflow_csv[1:])
    )
    output_bucket.upload_fileobj(input_overflow_csv_gz, "v2/2020/1/overflow/Jan-2020.csv.gz")

    with open(datadir / "v2" / "2019" / "12" / "organisationMetadata.json") as f:
        organisation_metadata_input_json = json.dumps(json.load(f)).encode("UTF-8")
    output_bucket.upload_fileobj(
        BytesIO(organisation_metadata_input_json), "v2/2019/12/organisationMetadata.json"
    )

    month = 12
    year = 2019

    config = DataPipelineConfig(
        input_bucket=s3_test_bucket,
        output_bucket=s3_test_bucket,
        organisation_list_bucket=s3_test_bucket,
        year=year,
        month=month,
        s3_endpoint_url=fake_s3_url,
    )

    try:
        main(config)
        actual_practice_metrics = _read_s3_json(
            output_bucket, f"{s3_path}{expected_practice_metrics_output_key}"
        )
        actual_organisation_metadata = _read_s3_json(
            output_bucket, f"{s3_path}{expected_organisation_metadata_output_key}"
        )
        actual_national_metrics = _read_s3_json(
            output_bucket, f"{s3_path}{expected_national_metrics_output_key}"
        )
        actual_transfers = _read_s3_parquet(
            output_bucket, f"{s3_path}{expected_transfers_output_key}"
        )

        assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
        assert (
            actual_organisation_metadata["practices"] == expected_organisation_metadata["practices"]
        )
        assert actual_national_metrics["metrics"] == expected_national_metrics["metrics"]

        assert actual_transfers == EXPECTED_TRANSFERS
    finally:
        output_bucket.objects.all().delete()
        output_bucket.delete()
        fake_s3.stop()
