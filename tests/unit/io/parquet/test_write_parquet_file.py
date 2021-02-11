from pathlib import Path
import pandas as pd
from gp2gp.io.parquet import write_parquet_file


def test_writes_dictionary(fs):
    content = [{"status": "open"}]
    file_path = "/foo/bar.parquet"
    fs.create_dir("/foo")

    write_parquet_file(content, file_path)

    expected = pd.DataFrame({"status": ["open"]})

    actual = pd.read_parquet(path=Path(file_path))

    pd.testing.assert_frame_equal(actual, expected)
