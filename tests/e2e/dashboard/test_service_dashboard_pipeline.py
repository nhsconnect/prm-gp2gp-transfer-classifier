import json
import gzip
import logging
import shutil
from io import BytesIO
from os import getenv

import boto3
from botocore.config import Config

from tests.builders.common import a_string
from subprocess import PIPE, Popen

logger = logging.getLogger(__name__)


def _read_json(path):
    return json.loads(path.read_text())


def _gzip_file(input_file_path):
    gzip_file_path = input_file_path.with_suffix(".gz")
    with open(input_file_path, "rb") as input_file:
        with gzip.open(gzip_file_path, "wb") as output_file:
            shutil.copyfileobj(input_file, output_file)
    return gzip_file_path


def _gzip_files(file_paths):
    return [_gzip_file(file_path) for file_path in file_paths]


def _csv_join(strings):
    return ",".join(strings)


def _csv_join_paths(paths):
    return _csv_join([str(p) for p in paths])


def _read_s3_json(bucket, key):
    f = BytesIO()
    bucket.download_fileobj(key, f)
    f.seek(0)
    return json.loads(f.read().decode("utf-8"))


def test_with_local_files(datadir):
    input_file_paths = _gzip_files(
        [datadir / "test_gp2gp_dec_2019.csv", datadir / "test_gp2gp_jan_2020.csv"]
    )
    practice_metadata_file_path = datadir / "practice_metadata.json"
    input_file_paths_str = _csv_join_paths(input_file_paths)

    practice_metrics_output_file_path = datadir / "practice_metrics_dec_2019.json"
    practice_metadata_output_file_path = datadir / "practice_metadata_dec_2019.json"

    expected_practice_metrics = _read_json(datadir / "expected_practice_metrics_dec_2019.json")
    expected_practice_metadata = _read_json(datadir / "expected_practice_metadata_dec_2019.json")

    month = 12
    year = 2019

    pipeline_command = f"\
        gp2gp-dashboard-pipeline --month {month}\
        --year {year}\
        --practice-list-file {practice_metadata_file_path}\
        --input-files {input_file_paths_str}\
        --practice-metrics-output-file {practice_metrics_output_file_path}\
        --practice-metadata-output-file {practice_metadata_output_file_path}\
    "

    process = Popen(pipeline_command, shell=True)
    process.wait()

    actual_practice_metrics = _read_json(practice_metrics_output_file_path)
    actual_practice_metadata = _read_json(practice_metadata_output_file_path)

    assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
    assert actual_practice_metadata["practices"] == expected_practice_metadata["practices"]


def test_with_s3_output(datadir):
    minio_data_dir = datadir / "minio"
    minio_address = "localhost:9001"
    minio_access_key = a_string()
    minio_secret_key = a_string()
    minio_region = "eu-west-2"
    minio_command = f"\
        minio server {minio_data_dir} \
        --address {minio_address} \
        --compat \
    "
    minio_env = {
        "MINIO_ACCESS_KEY": minio_access_key,
        "MINIO_SECRET_KEY": minio_secret_key,
        "MINIO_REGION_NAME": minio_region,
        "HOME": datadir,
    }

    minio_process = Popen(minio_command, env=minio_env, shell=True, stdout=PIPE, stderr=PIPE)

    s3 = boto3.resource(
        "s3",
        endpoint_url=f"http://{minio_address}",
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=minio_region,
    )

    output_bucket_name = "testbucket"
    output_bucket = s3.Bucket(output_bucket_name)
    output_bucket.create()

    input_file_paths = _gzip_files(
        [datadir / "test_gp2gp_dec_2019.csv", datadir / "test_gp2gp_jan_2020.csv"]
    )
    practice_metadata_file_path = datadir / "practice_metadata.json"
    input_file_paths_str = _csv_join_paths(input_file_paths)

    practice_metrics_output_key = "practice_metrics_dec_2019.json"
    practice_metadata_output_key = "practice_metadata_dec_2019.json"

    expected_practice_metrics = _read_json(datadir / "expected_practice_metrics_dec_2019.json")
    expected_practice_metadata = _read_json(datadir / "expected_practice_metadata_dec_2019.json")

    month = 12
    year = 2019

    pipeline_env = {
        "AWS_ACCESS_KEY_ID": minio_access_key,
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_DEFAULT_REGION": minio_region,
        "PATH": getenv("PATH"),
    }

    pipeline_command = f"\
        gp2gp-dashboard-pipeline --month {month}\
        --year {year}\
        --practice-list-file {practice_metadata_file_path}\
        --input-files {input_file_paths_str}\
        --output-bucket {output_bucket_name}\
        --practice-metrics-output-key {practice_metrics_output_key} \
        --practice-metadata-output-key {practice_metadata_output_key} \
        --s3-endpoint-url http://{minio_address} \
    "
    pipeline_process = Popen(
        pipeline_command, shell=True, env=pipeline_env, stdout=PIPE, stderr=PIPE
    )

    try:

        pipeline_process.wait()

        actual_practice_metrics = _read_s3_json(output_bucket, practice_metrics_output_key)
        actual_practice_metadata = _read_s3_json(output_bucket, practice_metadata_output_key)

        assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
        assert actual_practice_metadata["practices"] == expected_practice_metadata["practices"]
    finally:
        output_bucket.objects.all().delete()
        output_bucket.delete()
        minio_process.terminate()
        minio_process.wait()
        if minio_process.returncode != 0:
            logger.error(f"Minio stdout: {minio_process.stdout.read()}")
            logger.error(f"Minio stderr: {minio_process.stderr.read()}")
        if pipeline_process.returncode != 0:
            logger.error(f"Pipeline stdout: {pipeline_process.stdout.read()}")
            logger.error(f"Pipeline stderr: {pipeline_process.stderr.read()}")
