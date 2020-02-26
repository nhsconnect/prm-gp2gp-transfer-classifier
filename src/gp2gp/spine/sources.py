import csv
import gzip
from typing import BinaryIO, Iterator, List

from dateutil import parser

from gp2gp.spine.models import Message


def _parse_error_code(error):
    return None if error == "NONE" else int(error)


def _parse_message_ref(ref):
    return None if ref == "NotProvided" else ref


def read_spine_csv_gz_files(input_file_paths: List[str]) -> Iterator[Message]:
    for file_path in input_file_paths:
        with open(file_path, "rb") as f:
            yield from _read_spine_csv_gz(f)


def _read_spine_csv_gz(input_file: BinaryIO) -> Iterator[Message]:
    with gzip.open(input_file, "rt") as f:
        input_csv = csv.DictReader(f)
        yield from construct_messages_from_splunk_items(input_csv)


def construct_messages_from_splunk_items(items: Iterator[dict]) -> Iterator[Message]:
    for item in items:
        yield Message(
            time=parser.isoparse(item["_time"]),
            conversation_id=item["conversationID"],
            guid=item["GUID"],
            interaction_id=item["interactionID"],
            from_party_ods_code=item["fromNACS"],
            to_party_ods_code=item["toNACS"],
            message_ref=_parse_message_ref(item["messageRef"]),
            error_code=_parse_error_code(item["jdiEvent"]),
        )
