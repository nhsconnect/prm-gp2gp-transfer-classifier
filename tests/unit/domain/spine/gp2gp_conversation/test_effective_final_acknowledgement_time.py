from typing import List

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases
from tests.builders.common import a_datetime


def test_returns_none_when_request_has_been_made():
    gp2gp_messages: List[Message] = test_cases.request_made()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_none_when_request_has_been_acknowledged():
    gp2gp_messages: List[Message] = test_cases.request_acknowledged_successfully()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_none_when_ehr_has_been_returned():
    gp2gp_messages: List[Message] = test_cases.core_ehr_sent()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_none_given_duplicate_and_pending():
    gp2gp_messages: List[Message] = test_cases.acknowledged_duplicate_and_waiting_for_integration()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_when_conversation_has_concluded_successfully():
    acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.ehr_integrated_successfully(
        ehr_acknowledge_time=acknowledgement_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_when_record_is_suppressed():
    effective_final_acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.suppressed_ehr(
        ehr_acknowledge_time=effective_final_acknowledgement_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_when_conversation_concluded_with_failure():
    effective_final_acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.concluded_with_failure(
        ehr_acknowledge_time=effective_final_acknowledgement_time,
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_given_duplicate_and_success():
    effective_final_acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.ehr_integrated_after_duplicate(
        ehr_acknowledge_time=effective_final_acknowledgement_time
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_given_duplicate_and_failure():
    effective_final_acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.integration_failed_after_duplicate(
        ehr_acknowledge_time=effective_final_acknowledgement_time,
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_given_first_ehr_integrated_before_second_ehr_failed():
    effective_final_acknowledgement_time = a_datetime()

    gp2gp_messages: List[Message] = test_cases.first_ehr_integrated_before_second_ehr_failed(
        ehr_acknowledge_time=effective_final_acknowledgement_time,
    )

    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_given_first_ehr_integrated_after_second_ehr_failed():
    effective_final_acknowledgement_time = a_datetime()

    gp2gp_messages: List[Message] = test_cases.first_ehr_integrated_after_second_ehr_failed(
        ehr_acknowledge_time=effective_final_acknowledgement_time,
    )

    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_given_second_ehr_integrated_after_first_ehr_failed():
    effective_final_acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.second_ehr_integrated_after_first_ehr_failed(
        ehr_acknowledge_time=effective_final_acknowledgement_time,
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


def test_returns_correct_time_given_second_ehr_integrated_before_first_ehr_failed():
    effective_final_acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_cases.second_ehr_integrated_before_first_ehr_failed(
        ehr_acknowledge_time=effective_final_acknowledgement_time,
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = effective_final_acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected
