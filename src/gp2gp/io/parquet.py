from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.lib import PythonFile


def write_parquet_file(content_table: pa.Table, file_path: str):
    pq.write_table(content_table, file_path)


def upload_parquet_object(content_table: pa.Table, s3_object):
    out_buffer = BytesIO()
    out_file = PythonFile(out_buffer)
    pq.write_table(content_table, out_file)
    out_buffer.seek(0)
    s3_object.put(Body=out_buffer.getvalue())
