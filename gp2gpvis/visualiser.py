import csv
import fileinput
from argparse import ArgumentParser, Namespace
from datetime import datetime
from itertools import chain
from typing import List


class RowMessage:
    def __init__(self, row):
        self.time = datetime.fromisoformat(row["_time"].split(".")[0])
        self.conversation_id = row["conversationID"]
        self.guid = row["GUID"]
        self.interaction_id = row["interactionID"]
        self.from_party_asid = row["messageSender"]
        self.to_party_asid = row["messageRecipient"]
        self.message_ref = None if row["messageRef"] == "NotProvided" else row["messageRef"]
        self.error_code = None if row["jdiEvent"] == "NONE" else int(row["jdiEvent"])
        self.from_system = row.get("fromSystem")
        self.to_system = row.get("toSystem")


class ConversationFormatter:

    _PADDING_WIDTH = 9
    _PADDING = _PADDING_WIDTH * " "
    _HEADER_PADDING = (_PADDING_WIDTH - 4) * " "
    _ARROW_WIDTH = 56
    _CENTRAL_WIDTH = _ARROW_WIDTH + 6
    _INTERACTION_HEADER = _HEADER_PADDING + "Requester" + _ARROW_WIDTH * " " + "Sender"
    _SPACING_LINE = _PADDING + "|" + _CENTRAL_WIDTH * " " + "|"
    _GUID_CHARS_LENGTH = 5

    _EHR_REQUEST_STARTED = "urn:nhs:names:services:gp2gp/RCMR_IN010000UK05"
    _EHR_REQUEST_COMPLETED = "urn:nhs:names:services:gp2gp/RCMR_IN030000UK06"
    _APPLICATION_ACK = "urn:nhs:names:services:gp2gp/MCCI_IN010000UK13"
    _COMMON_POINT_TO_POINT = "urn:nhs:names:services:gp2gp/COPC_IN000001UK01"

    _INTERACTION_NAMES = {
        _EHR_REQUEST_STARTED: "GP2GP request",
        _EHR_REQUEST_COMPLETED: "Core EHR",
        _APPLICATION_ACK: "Acknowledgement",
        _COMMON_POINT_TO_POINT: "COPC",
    }

    _ERROR_MESSAGES = {
        6: "Not at surgery",
        7: "GP2GP disabled",
        9: "Unexpected EHR",
        10: "Failed to generate",
        11: "Failed to integrate",
        12: "Duplicate EHR",
        13: "Config issue",
        14: "Req not LM compliant",
        15: "ABA suppressed",
        17: "ABA wrong patient",
        18: "Req malformed",
        19: "Unauthorised req",
        20: "Spine error",
        21: "Extract malformed",
        23: "Sender not LM compliant",
        24: "SDS lookup",
        25: "Timeout",
        26: "Filed as attachment",
        28: "Wrong patient",
        29: "LM reassembly",
        30: "LM general failure",
        31: "Missing LM",
        99: "Unexpected",
    }

    def __init__(self, messages: List[RowMessage], minimal_output=False):
        first_message = messages[0]

        if first_message.interaction_id != self._EHR_REQUEST_STARTED:
            raise ValueError("First message was not a GP2GP request")

        self._messages = messages
        self._messages_by_guid = {message.guid: message for message in messages}
        self._conversation_id = first_message.conversation_id
        self._requesting_asid = first_message.from_party_asid
        self._sending_asid = first_message.to_party_asid
        self._requesting_system = first_message.from_system
        self._sending_system = first_message.to_system

        self._minimal = minimal_output

    def format(self):
        interactions = self._build_interaction_lines()
        lines = interactions if self._minimal else chain(self._build_meta_lines(), interactions)
        return "\n".join(lines)

    def _build_meta_lines(self):
        return [
            f"GP2GP Conversation: {self._conversation_id}",
            f"Requester: {self._requesting_asid} ({self._requesting_system})",
            f"Sender: {self._sending_asid} ({self._sending_system})",
            "",
        ]

    def _build_interaction_lines(self):
        yield self._INTERACTION_HEADER
        for message in self._messages:
            if message.conversation_id != self._conversation_id:
                error = f"{message.guid} not from conversation: {self._conversation_id}"
                raise ValueError(error)
            yield from self._build_message_lines(message)

    def _build_message_lines(self, message):
        yield self._SPACING_LINE
        yield self._message_description_line(message)
        if message.interaction_id == self._APPLICATION_ACK and message.error_code is not None:
            yield self._error_description_line(message.error_code)

    def _message_description_line(self, message):
        message_label = f" {self._message_description(message)} "
        content = f"{message_label:-^{self._ARROW_WIDTH}}"
        timestamp = message.time.strftime("%y-%m-%d %H:%M:%S")
        from_requester = message.from_party_asid == self._requesting_asid
        arrow = self._right_arrow(content) if from_requester else self._left_arrow(content)
        line = arrow
        if not self._minimal:
            line += f"   {timestamp}"
        return line

    def _message_description(self, message):
        interaction_name = self._INTERACTION_NAMES[message.interaction_id]
        short_guid = message.guid[: self._GUID_CHARS_LENGTH]
        message_description = f"({short_guid}) {interaction_name}"

        if message.interaction_id == self._APPLICATION_ACK:
            acked_guid = message.message_ref
            acked_message = self._messages_by_guid.get(acked_guid)
            if acked_message is None:
                message_description += " of non existent message!"
            else:
                acked_interaction = self._INTERACTION_NAMES[acked_message.interaction_id]
                short_acked_guid = acked_guid[: self._GUID_CHARS_LENGTH]
                message_description += f" of {acked_interaction} '{short_acked_guid}'"

        return message_description

    def _right_arrow(self, content):
        return f"{self._PADDING}| --{content}-> |"

    def _left_arrow(self, content):
        return f"{self._PADDING}| <-{content}-- |"

    def _error_description_line(self, error_code):
        error_message = self._ERROR_MESSAGES.get(error_code, "???")
        error_desc = f"Error code: {error_code} ({error_message})"
        return f"{self._PADDING}|{error_desc:^{self._CENTRAL_WIDTH}}|"


def read_rows(filepath):
    input_file = fileinput.input(files=[filepath])
    return list(csv.DictReader(input_file))


def format_csv_rows(rows, sort_messages=True, minimal_output=False):
    messages = [RowMessage(row) for row in rows]
    if sort_messages:
        messages = sorted(messages, key=lambda m: m.time)
    formatter = ConversationFormatter(messages, minimal_output)
    return formatter.format()


def parse_arguments() -> Namespace:
    parser = ArgumentParser(description="Visualise GP2GP spine messages")
    parser.add_argument("csv_filepath", help="Path to SPINE CSV, use '-' to read from stdin")
    return parser.parse_args()


def main():
    args = parse_arguments()
    rows = list(read_rows(args.csv_filepath))
    print(format_csv_rows(rows))
