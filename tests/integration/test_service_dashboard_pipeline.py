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
    input_file_paths_str = _csv_join_paths(input_file_paths)

    output_file_path = datadir / "dashboard_dec_2019.json"
    expected_practices = _read_json(datadir / "expected_practices_dec_2019.json")

    practice_ods_codes = ["A12345", "Z45678"]
    practice_ods_codes_str = _csv_join(practice_ods_codes)
    month = 12
    year = 2019

    pipeline_command = f"\
        gp2gp-dashboard-pipeline --month {month}\
        --year {year}\
        --ods-codes {practice_ods_codes_str}\
        --input-files {input_file_paths_str}\
        --output-file {output_file_path}\
    "

    process = subprocess.Popen(pipeline_command, shell=True)
    process.wait()

    actual = _read_json(output_file_path)

    assert actual["practices"] == expected_practices
