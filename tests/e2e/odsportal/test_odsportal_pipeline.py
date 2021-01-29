import subprocess
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from gp2gp.io.json import read_json_file
from urllib.parse import parse_qs
from tests.builders.file import gzip_file

HOST_NAME = "localhost"
PORT_NUMBER = 9000
PRACTICE_RESPONSE_CONTENT = (
    b'{"Organisations": [{"Name": "Test GP", "OrgId": "A12345"}, '
    b'{"Name": "Test GP 2", "OrgId": "B12345"}, '
    b'{"Name": "Test GP 3", "OrgId": "C12345"}]}'
)
CCG_RESPONSE_CONTENT = (
    b'{"Organisations": [{"Name": "Test CCG", "OrgId": "12A"}, '
    b'{"Name": "Test CCG 2", "OrgId": "13B"}, '
    b'{"Name": "Test CCG 3", "OrgId": "14C"}]}'
)


class MockRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_params = parse_qs(self.path[2:])
        primary_role = parsed_params["PrimaryRoleId"][0]

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(
            PRACTICE_RESPONSE_CONTENT if primary_role == "RO177" else CCG_RESPONSE_CONTENT
        )


def test_ods_portal_pipeline(datadir):
    output_file_path = datadir / "organisation_metadata.json"
    input_mapping_file = gzip_file(datadir / "asid_mapping.csv")

    expected_practices = read_json_file(datadir / "expected_practice_list.json")
    expected_ccgs = read_json_file(datadir / "expected_ccg_list.json")

    httpd = HTTPServer((HOST_NAME, PORT_NUMBER), MockRequestHandler)

    thread = threading.Thread(target=httpd.serve_forever)
    thread.daemon = True
    thread.start()

    pipeline_command = f"\
        ods-portal-pipeline\
        --output-file {output_file_path}\
        --mapping-file {input_mapping_file}\
        --search-url {f'http://{HOST_NAME}:{PORT_NUMBER}'}\
    "

    process = subprocess.Popen(pipeline_command, shell=True)
    process.wait()

    httpd.server_close()
    actual = read_json_file(output_file_path)

    assert actual["practices"] == expected_practices
    assert actual["ccgs"] == expected_ccgs
