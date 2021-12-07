from prmdata.domain.spine.message import (
    APPLICATION_ACK,
    COMMON_POINT_TO_POINT,
    EHR_REQUEST_COMPLETED,
    EHR_REQUEST_STARTED,
)
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


def test_is_request_started_returns_true_when_interaction_is_ehr_req_started():
    message = build_message(interaction_id=EHR_REQUEST_STARTED)

    expected = True

    actual = message.is_ehr_request_started()

    assert actual == expected


def test_is_request_started_returns_false_when_interaction_is_not_req_started():
    message = build_message(interaction_id=APPLICATION_ACK)

    expected = False

    actual = message.is_ehr_request_started()

    assert actual == expected


def test_is_acknowledgement_of_returns_false_when_message_is_not_an_ack():
    message = build_message(interaction_id=COMMON_POINT_TO_POINT, message_ref="123")
    other_message = build_message(guid="123")

    expected = False

    actual = message.is_acknowledgement_of(other_message)

    assert actual == expected


def test_is_acknowledgement_of_returns_false_when_message_ref_is_wrong():
    message = build_message(interaction_id=APPLICATION_ACK, message_ref="123")
    other_message = build_message(guid="456")

    expected = False

    actual = message.is_acknowledgement_of(other_message)

    assert actual == expected


def test_is_acknowledgement_of_returns_true_when_given_message_with_matching_ref():
    message = build_message(interaction_id=APPLICATION_ACK, message_ref="123")
    other_message = build_message(guid="123")

    expected = True

    actual = message.is_acknowledgement_of(other_message)

    assert actual == expected


def test_is_copc_returns_true_when_interaction_is_copc():
    message = build_message(interaction_id=COMMON_POINT_TO_POINT)

    expected = True

    actual = message.is_copc()

    assert actual == expected


def test_is_copc_returns_false_when_interaction_is__not_copc():
    message = build_message(interaction_id=APPLICATION_ACK)

    expected = False

    actual = message.is_copc()

    assert actual == expected
