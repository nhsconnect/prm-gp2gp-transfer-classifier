from prmdata.domain.gp2gp.transfer import derive_transfers
from tests.builders.spine import build_parsed_conversation
from tests.unit.domain.gp2gp.test_derive_transfer import _assert_attributes


def test_extracts_request_completed_ack_codes_for_conversations():
    conversations = [
        build_parsed_conversation(id="1234", request_completed_ack_codes=[None]),
        build_parsed_conversation(id="3456", request_completed_ack_codes=[12, 12, None]),
    ]

    actual = derive_transfers(conversations)

    expected_request_completed_ack_codes = [[None], [12, 12, None]]

    _assert_attributes("request_completed_ack_codes", actual, expected_request_completed_ack_codes)
