from typing import Iterator, List, Iterable

from dateutil import parser

from gp2gp.io.csv import read_gzip_csv
from gp2gp.spine.models import Message


def _parse_error_code(error):
    return None if error == "NONE" else int(error)


def _parse_message_ref(ref):
    return None if ref == "NotProvided" else ref


def read_spine_csv_gz_files(input_file_paths: List[str]) -> Iterator[Message]:
    for file_path in input_file_paths:
        items = read_gzip_csv(file_path)
        yield from construct_messages_from_splunk_items(items)


def construct_messages_from_splunk_items(items: Iterable[dict]) -> Iterator[Message]:
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
