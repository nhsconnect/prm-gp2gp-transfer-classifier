from datetime import datetime
import json
import logging
from io import BytesIO
from os import getenv
from threading import Thread

import boto3
from botocore.config import Config
from moto.server import DomainDispatcherApplication, create_backend_app
from werkzeug.serving import make_server
from tests.builders.file import gzip_file
import pyarrow.parquet as pq

from subprocess import check_output

logger = logging.getLogger(__name__)

EXPECTED_TRANSFERS = {
    "conversation_id": ["integrated-within-8-days", "integrated-beyond-8-days", "abc"],
    "date_completed": [
        datetime(2019, 12, 6, 8, 41, 48, 337000),
        datetime(2019, 12, 15, 8, 41, 48, 337000),
        datetime(2020, 1, 1, 8, 41, 48, 337000),
    ],
    "date_requested": [
        datetime(2019, 12, 1, 18, 2, 29, 985000),
        datetime(2019, 12, 5, 18, 2, 29, 985000),
        datetime(2019, 12, 30, 18, 2, 29, 985000),
    ],
    "final_error_code": [None, None, None],
    "intermediate_error_codes": [[], [], []],
    "requesting_practice_asid": ["123456789123", "123456789123", "123456789123"],
    "sending_practice_asid": ["003456789123", "003456789123", "003456789123"],
    "sla_duration": [398306, 830306, 139106],
    "status": ["INTEGRATED", "INTEGRATED", "INTEGRATED"],
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


def test_with_local_files(datadir):
    input_file_paths = _gzip_files(
        [datadir / "test_gp2gp_dec_2019.csv", datadir / "test_gp2gp_jan_2020.csv"]
    )
    organisation_metadata_file_path = datadir / "organisation_metadata.json"
    input_file_paths_str = _csv_join_paths(input_file_paths)

    practice_metrics_output_file_path = datadir / "12-2019-practiceMetrics.json"
    organisation_metadata_output_file_path = datadir / "12-2019-organisationMetadata.json"
    national_metrics_output_file_path = datadir / "12-2019-nationalMetrics.json"
    transfers_output_file_path = datadir / "12-2019-transfers.parquet"

    expected_practice_metrics = _read_json(datadir / "expected_practice_metrics_dec_2019.json")
    expected_organisation_metadata = _read_json(
        datadir / "expected_organisation_metadata_dec_2019.json"
    )
    expected_national_metrics = _read_json(datadir / "expected_national_metrics_dec_2019.json")

    month = 12
    year = 2019

    pipeline_command = f"\
        gp2gp-dashboard-pipeline --month {month}\
        --year {year}\
        --organisation-list-file {organisation_metadata_file_path}\
        --input-files {input_file_paths_str}\
        --output-directory {datadir}\
    "

    pipeline_output = check_output(pipeline_command, shell=True)
    logger.debug(pipeline_output)

    actual_practice_metrics = _read_json(practice_metrics_output_file_path)
    actual_organisation_metadata = _read_json(organisation_metadata_output_file_path)
    actual_national_metrics = _read_json(national_metrics_output_file_path)
    actual_transfers = _read_parquet(transfers_output_file_path)

    assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
    assert actual_organisation_metadata["practices"] == expected_organisation_metadata["practices"]
    assert actual_national_metrics["metrics"] == expected_national_metrics["metrics"]
    assert actual_transfers == EXPECTED_TRANSFERS


def test_with_s3_output(datadir):
    fake_s3_host = "127.0.0.1"
    fake_s3_port = 8887
    fake_s3_url = f"http://{fake_s3_host}:{fake_s3_port}"
    fake_s3_access_key = "testing"
    fake_s3_secret_key = "testing"
    fake_s3_region = "us-west-1"

    fake_s3 = _build_fake_s3(fake_s3_host, fake_s3_port)
    fake_s3.start()

    s3 = boto3.resource(
        "s3",
        endpoint_url=fake_s3_url,
        aws_access_key_id=fake_s3_access_key,
        aws_secret_access_key=fake_s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=fake_s3_region,
    )

    output_bucket_name = "testbucket"
    output_bucket = s3.Bucket(output_bucket_name)
    output_bucket.create()

    input_file_paths = _gzip_files(
        [datadir / "test_gp2gp_dec_2019.csv", datadir / "test_gp2gp_jan_2020.csv"]
    )
    organisation_metadata_file_path = datadir / "organisation_metadata.json"
    input_file_paths_str = _csv_join_paths(input_file_paths)

    expected_practice_metrics_output_key = "practiceMetrics.json"
    expected_organisation_metadata_output_key = "organisationMetadata.json"
    expected_national_metrics_output_key = "nationalMetrics.json"
    expected_transfers_output_key = "transfers.parquet"

    expected_practice_metrics = _read_json(datadir / "expected_practice_metrics_dec_2019.json")
    expected_organisation_metadata = _read_json(
        datadir / "expected_organisation_metadata_dec_2019.json"
    )
    expected_national_metrics = _read_json(datadir / "expected_national_metrics_dec_2019.json")

    month = 12
    year = 2019

    pipeline_env = {
        "AWS_ACCESS_KEY_ID": fake_s3_access_key,
        "AWS_SECRET_ACCESS_KEY": fake_s3_secret_key,
        "AWS_DEFAULT_REGION": fake_s3_region,
        "PATH": getenv("PATH"),
    }

    pipeline_command = f"\
        gp2gp-dashboard-pipeline --month {month}\
        --year {year}\
        --organisation-list-file {organisation_metadata_file_path}\
        --input-files {input_file_paths_str}\
        --output-bucket {output_bucket_name}\
        --s3-endpoint-url {fake_s3_url} \
    "
    pipeline_output = check_output(pipeline_command, shell=True, env=pipeline_env)

    try:
        s3_path = "v2/2019/12/"
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
        logger.debug(pipeline_output)
