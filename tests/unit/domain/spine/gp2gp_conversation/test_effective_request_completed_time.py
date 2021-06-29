from typing import List

import pytest

from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases
from tests.builders.common import a_datetime
from prmdata.domain.spine.message import Message


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.request_made,
        test_cases.request_acknowledged_successfully,
        test_cases.core_ehr_sent,
        test_cases.acknowledged_duplicate_and_waiting_for_integration,
    ],
)
def test_returns_none_when_transfer_in_progress(test_case):
    gp2gp_messages: List[Message] = test_case()
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = None

    actual = conversation.effective_request_completed_time()

    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    [
        test_cases.ehr_integrated_successfully,
        test_cases.suppressed_ehr,
        test_cases.concluded_with_failure,
        test_cases.ehr_integrated_after_duplicate,
        test_cases.integration_failed_after_duplicate,
        test_cases.first_ehr_integrated_before_second_ehr_failed,
        test_cases.first_ehr_integrated_after_second_ehr_failed,
        test_cases.second_ehr_integrated_after_first_ehr_failed,
        test_cases.second_ehr_integrated_before_first_ehr_failed,
    ],
)
def test_returns_correct_time_when_conversation_has_concluded(test_case):
    request_completed_time = a_datetime()
    gp2gp_messages: List[Message] = test_case(request_completed_time=request_completed_time)
    conversation = Gp2gpConversation.from_messages(gp2gp_messages)

    expected = request_completed_time

    actual = conversation.effective_request_completed_time()

    assert actual == expected