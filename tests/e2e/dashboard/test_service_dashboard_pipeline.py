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

from subprocess import PIPE, Popen

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

    practice_metrics_output_file_path = datadir / "practice_metrics_dec_2019.json"
    organisation_metadata_output_file_path = datadir / "organisation_metadata_dec_2019.json"
    transfers_output_file_path = datadir / "transfers_dec_2019.json"

    expected_practice_metrics = _read_json(datadir / "expected_practice_metrics_dec_2019.json")
    expected_organisation_metadata = _read_json(
        datadir / "expected_organisation_metadata_dec_2019.json"
    )
    expected_transfers = _read_json(
        datadir / "expected_transfers_dec_2019.json"
    )

    month = 12
    year = 2019

    pipeline_command = f"\
        gp2gp-dashboard-pipeline --month {month}\
        --year {year}\
        --organisation-list-file {organisation_metadata_file_path}\
        --input-files {input_file_paths_str}\
        --practice-metrics-output-file {practice_metrics_output_file_path}\
        --organisation-metadata-output-file {organisation_metadata_output_file_path}\
        --transfers-output-file {transfers_output_file_path}\
    "

    process = Popen(pipeline_command, shell=True)
    process.wait()

    actual_practice_metrics = _read_json(practice_metrics_output_file_path)
    actual_organisation_metadata = _read_json(organisation_metadata_output_file_path)
    actual_transfers = _read_json(transfers_output_file_path)

    assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
    assert actual_organisation_metadata["practices"] == expected_organisation_metadata["practices"]
    assert actual_transfers == expected_transfers


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

    practice_metrics_output_key = "practice_metrics_dec_2019.json"
    organisation_metadata_output_key = "organisation_metadata_dec_2019.json"
    transfers_output_key = "transfers_dec_2019.json"

    expected_practice_metrics = _read_json(datadir / "expected_practice_metrics_dec_2019.json")
    expected_organisation_metadata = _read_json(
        datadir / "expected_organisation_metadata_dec_2019.json"
    )
    expected_transfers = _read_json(datadir / "expected_transfers_dec_2019.json")

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
        --practice-metrics-output-key {practice_metrics_output_key} \
        --organisation-metadata-output-key {organisation_metadata_output_key} \
        --transfers-output-key {transfers_output_key} \
        --s3-endpoint-url {fake_s3_url} \
    "
    pipeline_process = Popen(
        pipeline_command, shell=True, env=pipeline_env, stdout=PIPE, stderr=PIPE
    )

    try:

        pipeline_process.wait()

        actual_practice_metrics = _read_s3_json(output_bucket, practice_metrics_output_key)
        actual_organisation_metadata = _read_s3_json(
            output_bucket, organisation_metadata_output_key
        )
        actual_transfers = _read_s3_json(
            output_bucket, transfers_output_key
        )

        assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
        assert (
            actual_organisation_metadata["practices"] == expected_organisation_metadata["practices"]
        )
        assert actual_transfers == expected_transfers
    finally:
        output_bucket.objects.all().delete()
        output_bucket.delete()
        fake_s3.stop()
        if pipeline_process.returncode != 0:
            logger.error(f"Pipeline stdout: {pipeline_process.stdout.read()}")
            logger.error(f"Pipeline stderr: {pipeline_process.stderr.read()}")
