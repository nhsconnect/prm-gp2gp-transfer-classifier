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
