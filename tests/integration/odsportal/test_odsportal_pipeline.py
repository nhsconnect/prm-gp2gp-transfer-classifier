import subprocess
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from gp2gp.io.json import read_json_file


HOST_NAME = "localhost"
PORT_NUMBER = 9000
RESPONSE_CONTENT = (
    b'{"Organisations": [{"Name": "Test GP", "OrgId": "A12345"}, '
    b'{"Name": "Test GP 2", "OrgId": "B12345"}, '
    b'{"Name": "Test GP 3", "OrgId": "C12345"}]}'
)


class MockRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(RESPONSE_CONTENT)


def test_ods_portal_pipeline(datadir):
    output_file_path = datadir / "practice_metadata.json"

    expected_practices = read_json_file(datadir / "expected_practice_list.json")

    httpd = HTTPServer((HOST_NAME, PORT_NUMBER), MockRequestHandler)

    thread = threading.Thread(target=httpd.serve_forever)
    thread.daemon = True
    thread.start()

    pipeline_command = f"\
        ods-portal-pipeline\
        --output-file {output_file_path}\
        --search-url {f'http://{HOST_NAME}:{PORT_NUMBER}'}\
    "

    process = subprocess.Popen(pipeline_command, shell=True)
    process.wait()

    httpd.server_close()

    actual = read_json_file(output_file_path)

    assert actual["practices"] == expected_practices
