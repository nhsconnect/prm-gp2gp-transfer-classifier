from gp2gp.io.parquet import write_parquet_file
import pyarrow.parquet as pq
import pyarrow as pa


def test_writes_tuple(tmp_path):
    file_path = tmp_path / "bar.parquet"

    content_table = pa.table([["open"]], ["status"])

    write_parquet_file(content_table, str(file_path))

    expected = pa.table([["open"]], ["status"])

    actual = pq.read_table(file_path)

    assert actual.equals(expected)
