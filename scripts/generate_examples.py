from types import FunctionType
from typing import get_type_hints, List, Iterable, Tuple

import tests.builders.test_cases as test_cases
from prmdata.domain.spine.message import Message


def gather_examples(module) -> Iterable[Tuple[str, List[Message]]]:
    for name, value in module.__dict__.items():
        if isinstance(value, FunctionType):
            type_hints = get_type_hints(value)
            return_type = type_hints.get("return")
            if return_type == List[Message]:
                yield name, value()

def generate_examples():
    for name, messages in gather_examples(test_cases):
        print(name)
        #print(f"\n\nGP2GP example: {name}\n")
        #print(format_conversation(messages))