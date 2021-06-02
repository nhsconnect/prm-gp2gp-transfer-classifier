import gzip
import shutil
from io import BytesIO


def _build_csv_contents(header, rows):
    def build_line(values):
        return ",".join(values)

    header_line = build_line(header)
    row_lines = [build_line(row) for row in rows]

    return "\n".join([header_line] + row_lines)


def _build_gzip_buffer(contents):
    buffer = BytesIO()
    with gzip.open(buffer, "wt") as f:
        f.write(contents)
    buffer.seek(0)
    return buffer


def build_gzip_csv(header, rows):
    contents = _build_csv_contents(header, rows)
    buffer = _build_gzip_buffer(contents)
    return buffer.getvalue()


def read_file_to_gzip_buffer(input_file_path):
    gzip_buffer = BytesIO()
    with open(input_file_path, "rb") as input_file:
        with gzip.open(gzip_buffer, "wb") as output_file:
            shutil.copyfileobj(input_file, output_file)
    gzip_buffer.seek(0)
    return gzip_buffer
