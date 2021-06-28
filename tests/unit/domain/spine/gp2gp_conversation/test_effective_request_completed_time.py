from typing import List

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases
from tests.builders.common import a_datetime
from prmdata.domain.spine.message import Message


def test_returns_none_when_request_has_been_made():
    gp2gp_messages: List[Message] = test_cases.request_made()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_none_when_request_has_been_acknowledged():
    gp2gp_messages: List[Message] = test_cases.request_acknowledged_successfully()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_none_when_ehr_has_been_returned():
    gp2gp_messages: List[Message] = test_cases.core_ehr_sent()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_none_given_duplicate_and_pending():
    gp2gp_messages: List[Message] = test_cases.acknowledged_duplicate_and_waiting_for_integration()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_when_conversation_has_concluded_successfully():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.ehr_integrated_successfully(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_when_record_is_suppressed():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.suppressed_ehr(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_when_conversation_concluded_with_failure():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.concluded_with_failure(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_duplicate_and_success():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.ehr_integrated_after_duplicate(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_duplicate_and_failure():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.integration_failed_after_duplicate(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_first_ehr_integrated_before_second_ehr_failed():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.first_ehr_integrated_before_second_ehr_failed(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_first_ehr_integrated_after_second_ehr_failed():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.first_ehr_integrated_after_second_ehr_failed(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_second_ehr_integrated_after_first_ehr_failed():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.second_ehr_integrated_after_first_ehr_failed(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected


def test_returns_correct_time_given_second_ehr_integrated_before_first_ehr_failed():
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.second_ehr_integrated_before_first_ehr_failed(
        request_completed_time=request_completed_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected
