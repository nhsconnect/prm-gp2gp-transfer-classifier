from io import BytesIO
from pathlib import Path
from typing import List
import pandas as pd


def write_parquet_file(content: List[dict], file_path: str):
    df = pd.DataFrame(content)
    df.to_parquet(path=Path(file_path))


def upload_parquet_object(content: List[dict], s3_object):
    df = pd.DataFrame(content)
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    s3_object.put(Body=out_buffer.getvalue())
