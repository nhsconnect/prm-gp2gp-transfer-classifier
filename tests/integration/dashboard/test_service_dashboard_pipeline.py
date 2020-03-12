import json
import subprocess
import gzip
import shutil


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


def test_service_dashboard_pipeline(datadir):
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

    process = subprocess.Popen(pipeline_command, shell=True)
    process.wait()

    actual_practice_metrics = _read_json(practice_metrics_output_file_path)
    actual_practice_metadata = _read_json(practice_metadata_output_file_path)

    assert actual_practice_metrics["practices"] == expected_practice_metrics["practices"]
    assert actual_practice_metadata["practices"] == expected_practice_metadata["practices"]
