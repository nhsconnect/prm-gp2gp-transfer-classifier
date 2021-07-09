from typing import List

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from prmdata.domain.spine.message import Message
from tests.builders import test_cases
from tests.builders.common import a_datetime


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_made,
        test_cases.request_acknowledged_successfully,
        test_cases.core_ehr_sent,
        test_cases.acknowledged_duplicate_and_waiting_for_integration,
        test_cases.pending_integration_with_acked_large_message_fragments,
    ],
)
def test_returns_none_when_transfer_in_progress(test_case):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.ehr_integrated_successfully,
        test_cases.ehr_suppressed,
        test_cases.ehr_integration_failed,
        test_cases.ehr_integrated_after_duplicate,
        test_cases.integration_failed_after_duplicate,
        test_cases.first_ehr_integrated_before_second_ehr_failed,
        test_cases.first_ehr_integrated_after_second_ehr_failed,
        test_cases.second_ehr_integrated_after_first_ehr_failed,
        test_cases.second_ehr_integrated_before_first_ehr_failed,
        test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs,
        test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack,
        test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack,
    ],
)
def test_returns_correct_time_when_conversation_has_concluded(test_case):
    acknowledgement_time = a_datetime()
    gp2gp_messages: List[Message] = test_case(
        ehr_acknowledge_time=acknowledgement_time,
    )
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = acknowledgement_time

    actual = conversation.effective_final_acknowledgement_time()

    assert actual == expected
