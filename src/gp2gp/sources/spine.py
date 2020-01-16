import gzip
from typing import Iterator, BinaryIO
import csv
from dateutil import parser

from gp2gp.models.spine import Message


def read_spine_csv_gz(input_file: BinaryIO) -> Iterator[str]:
    with gzip.open(input_file, "rt") as f:
        input_csv = csv.DictReader(f)
        for row in input_csv:
            yield Message(
                time=parser.isoparse(row["_time"]),
                conversation_id=row["conversationID"],
                guid=row["GUID"],
            )
