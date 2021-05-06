from prmdata.domain.spine.message import EHR_REQUEST_COMPLETED, APPLICATION_ACK
from tests.builders.spine import build_message


def test_is_ehr_request_completed_returns_true_when_interaction_is_ehr_req_completed():
    message = build_message(interaction_id=EHR_REQUEST_COMPLETED)

    expected = True

    actual = message.is_ehr_request_completed()

    assert actual == expected


def test_is_ehr_request_completed_returns_false_when_interaction_is_not_ehr_req_completed():
    message = build_message(interaction_id=APPLICATION_ACK)

    expected = False

    actual = message.is_ehr_request_completed()

    assert actual == expected
