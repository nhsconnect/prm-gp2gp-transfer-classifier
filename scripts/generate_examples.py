from dataclasses import dataclass
from types import FunctionType
from typing import get_type_hints, List, Iterable

import tests.builders.test_cases as test_cases
from gp2gpvis.visualiser import format_csv_rows
from prmdata.domain.spine.message import Message


@dataclass
class Gp2gpExample:
    name: str
    description: str
    messages: List[Message]

    def spine_rows(self) -> List[dict]:
        return [
            {
                "_time": message.time.isoformat(),
                "conversationID": message.conversation_id,
                "GUID": message.guid,
                "interactionID": message.interaction_id,
                "messageSender": message.from_party_asid,
                "messageRecipient": message.to_party_asid,
                "messageRef": "NotProvided" if message.message_ref is None else message.message_ref,
                "jdiEvent": "NONE" if message.error_code is None else str(message.error_code),
                "toSystem": message.to_system,
                "fromSystem": message.from_system,
            }
            for message in self.messages
        ]


def gather_examples(module) -> Iterable[Gp2gpExample]:
    for name, value in module.__dict__.items():
        if isinstance(value, FunctionType):
            type_hints = get_type_hints(value)
            return_type = type_hints.get("return")
            if return_type == List[Message]:
                doc = value.__doc__ or ""
                doc_lines = [line.strip() for line in doc.splitlines()]
                desc = "\n".join([line for line in doc_lines if line != ""])
                yield Gp2gpExample(name=name.replace("_", " "), description=desc, messages=value())


def generate_examples():
    print("# Example GP2GP Test Cases")
    print("This document describes the scenarios used to test the transfer classifier.")
    print("These examples are auto-generated from source.")
    print("Note: Many are quite rare edge cases.")
    for example in gather_examples(test_cases):
        print(f"\n\n## {example.name.title()}\n")
        print(example.description)
        print("\n```")
        print(format_csv_rows(example.spine_rows(), sort_messages=False, minimal_output=True))
        print("```")


if __name__ == "__main__":
    generate_examples()
