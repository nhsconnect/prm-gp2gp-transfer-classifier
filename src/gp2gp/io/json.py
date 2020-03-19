import json
from datetime import datetime
from pathlib import Path


def _serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type {} is not JSON serializable".format(type(obj)))


def write_json_file(content: dict, file_path: str):
    json_string = json.dumps(content, default=_serialize_datetime)
    path = Path(file_path)
    path.write_text(json_string)


def read_json_file(file_path: str) -> dict:
    path = Path(file_path)
    json_string = path.read_text()
    return json.loads(json_string)


def upload_json_object(content: dict, s3_object):
    json_string = json.dumps(content, default=_serialize_datetime)
    body = bytes(json_string.encode("UTF-8"))
    s3_object.put(Body=body, ContentType="application/json")
