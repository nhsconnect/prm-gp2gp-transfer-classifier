import json
from dataclasses import asdict
from datetime import datetime
from typing import List, Mapping, TextIO


def _camelize(string):
    components = string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def _camelize_dict(obj):
    if isinstance(obj, List):
        return [_camelize_dict(i) for i in obj]
    elif isinstance(obj, Mapping):
        return {_camelize(k): _camelize_dict(v) for k, v in obj.items()}
    return obj


def _serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type {} is not JSON serializable".format(type(obj)))


def serialize_as_json(obj):
    data_dict = asdict(obj)
    camelcase_data_dict = _camelize_dict(data_dict)
    return json.dumps(camelcase_data_dict, default=_serialize_datetime)


def write_as_json(obj, outfile: TextIO):
    json_output = serialize_as_json(obj)
    outfile.write(json_output)
