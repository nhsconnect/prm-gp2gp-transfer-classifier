from typing import List, Mapping


def _camelize(string):
    components = string.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def camelize_dict(obj):
    if isinstance(obj, List):
        return [camelize_dict(i) for i in obj]
    elif isinstance(obj, Mapping):
        return {_camelize(k): camelize_dict(v) for k, v in obj.items()}
    return obj
