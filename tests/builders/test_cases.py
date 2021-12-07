from datetime import timedelta
from typing import List

from prmdata.domain.spine.message import (
    APPLICATION_ACK,
    COMMON_POINT_TO_POINT,
    EHR_REQUEST_COMPLETED,
    EHR_REQUEST_STARTED,
    Message,
)
from tests.builders.common import a_datetime, a_string, an_integer

DUPLICATE_EHR_ERROR = 12
SUPPRESSED_EHR_ERROR = 15


class GP2GPTestCase:
    def __init__(self, **kwargs):
        self._messages = []
        self._conversation_id = kwargs.get("conversation_id", a_string())
        self._requesting_asid = kwargs.get("requesting_asid", a_string())
        self._sending_asid = kwargs.get("sending_asid", a_string())
        self._requesting_system = kwargs.get("requesting_system", a_string())
        self._sending_system = kwargs.get("sending_system", a_string())

    def with_request(self, **kwargs):
        self._messages.append(
            Message(
                time=kwargs.get("time", a_datetime()),
                conversation_id=self._conversation_id,
                guid=self._conversation_id,
                interaction_id=EHR_REQUEST_STARTED,
                from_party_asid=self._requesting_asid,
                to_party_asid=self._sending_asid,
                message_ref=None,
                error_code=None,
                from_system=self._requesting_system,
                to_system=self._sending_system,
            )
        )
        return self

    def with_sender_acknowledgement(self, *, message_ref, **kwargs):
        self._messages.append(
            Message(
                time=kwargs.get("time", a_datetime()),
                conversation_id=self._conversation_id,
                guid=a_string(),
                interaction_id=APPLICATION_ACK,
                from_party_asid=self._sending_asid,
                to_party_asid=self._requesting_asid,
                message_ref=message_ref,
                error_code=kwargs.get("error_code", None),
                from_system=self._sending_system,
                to_system=self._requesting_system,
            )
        )
        return self

    def with_requester_acknowledgement(self, *, message_ref, **kwargs):
        self._messages.append(
            Message(
                time=kwargs.get("time", a_datetime()),
                conversation_id=self._conversation_id,
                guid=a_string(),
                interaction_id=APPLICATION_ACK,
                from_party_asid=self._requesting_asid,
                to_party_asid=self._sending_asid,
                message_ref=message_ref,
                error_code=kwargs.get("error_code", None),
                from_system=self._requesting_system,
                to_system=self._sending_system,
            )
        )
        return self

    def with_core_ehr(self, **kwargs):
        self._messages.append(
            Message(
                time=kwargs.get("time", a_datetime()),
                conversation_id=self._conversation_id,
                guid=kwargs.get("guid", a_string()),
                interaction_id=EHR_REQUEST_COMPLETED,
                from_party_asid=self._sending_asid,
                to_party_asid=self._requesting_asid,
                message_ref=None,
                error_code=None,
                from_system=self._sending_system,
                to_system=self._requesting_system,
            )
        )
        return self

    def with_copc_fragment_continue(self, **kwargs):
        self._messages.append(
            Message(
                time=kwargs.get("time", a_datetime()),
                conversation_id=self._conversation_id,
                guid=a_string(),
                interaction_id=COMMON_POINT_TO_POINT,
                from_party_asid=self._requesting_asid,
                to_party_asid=self._sending_asid,
                message_ref=None,
                error_code=None,
                from_system=self._requesting_system,
                to_system=self._sending_system,
            )
        )
        return self

    def with_copc_fragment(self, **kwargs):
        self._messages.append(
            Message(
                time=kwargs.get("time", a_datetime()),
                conversation_id=self._conversation_id,
                guid=kwargs.get("guid", a_string()),
                interaction_id=COMMON_POINT_TO_POINT,
                from_party_asid=self._sending_asid,
                to_party_asid=self._requesting_asid,
                message_ref=None,
                error_code=None,
                from_system=self._sending_system,
                to_system=self._requesting_system,
            )
        )
        return self

    def build(self):
        return self._messages


def request_made(**kwargs) -> List[Message]:
    """
    In this example, the requester has send a GP2GP request,
    but the sender is yet to acknowledge.
    """
    request_time = kwargs.get("request_sent_date", a_datetime())

    return (
        GP2GPTestCase(
            conversation_id=kwargs.get("conversation_id", a_string()),
            requesting_asid=kwargs.get("requesting_asid", a_string()),
            sending_asid=kwargs.get("sending_asid", a_string()),
            requesting_system=kwargs.get("requesting_system", a_string()),
            sending_system=kwargs.get("sending_system", a_string()),
        )
        .with_request(time=request_time)
        .build()
    )


def request_acknowledged_successfully(**kwargs) -> List[Message]:
    """
    In this example, the sender responded to the initial request
    with a successful ack, but is yet to send the core EHR.
    """
    conversation_id = a_string()
    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(
            message_ref=conversation_id,
            time=kwargs.get("sender_ack_time", a_datetime()),
        )
        .build()
    )


def request_acknowledged_with_error(**kwargs) -> List[Message]:
    """
    In this example, the sender responded to the initial request with an error code.
    """
    conversation_id = a_string()
    request_ack_error = kwargs.get("error_code", 19)
    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id, error_code=request_ack_error)
        .build()
    )


def core_ehr_sent(**kwargs) -> List[Message]:
    """
    In this example, the sender has returned an EHR, but the requester is yet to acknowledge.
    """
    conversation_id = a_string()
    req_complete_time = kwargs.get("request_completed_time", a_datetime())

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(
            message_ref=conversation_id, time=req_complete_time - timedelta(hours=1)
        )
        .with_core_ehr(time=req_complete_time)
        .build()
    )


def core_ehr_sent_with_sender_error(**kwargs) -> List[Message]:
    """
    In this example, the sender responded to the initial request with an error code,
    but still sent a core EHR message.
    """
    conversation_id = a_string()
    request_ack_error = kwargs.get("error_code", 99)

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id, error_code=request_ack_error)
        .with_core_ehr()
        .build()
    )


def acknowledged_duplicate_and_waiting_for_integration() -> List[Message]:
    """
    In this example, the sender returned two copies of the EHR,
    one of which the requester has rejected as duplicate,
    the other which is yet to be acknowledged.
    """
    conversation_id = a_string()
    ehr_guid = a_string()
    duplicate_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid)
        .with_core_ehr(guid=duplicate_ehr_guid)
        .with_requester_acknowledgement(
            message_ref=duplicate_ehr_guid, error_code=DUPLICATE_EHR_ERROR
        )
        .build()
    )


def only_acknowledged_duplicates() -> List[Message]:
    """
    In this example, the sender returned two copies of the EHR,
    both of which the requester has rejected as duplicates.
    """
    conversation_id = a_string()
    ehr_guid = a_string()
    duplicate_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid)
        .with_core_ehr(guid=duplicate_ehr_guid)
        .with_requester_acknowledgement(
            message_ref=duplicate_ehr_guid, error_code=DUPLICATE_EHR_ERROR
        )
        .with_requester_acknowledgement(message_ref=ehr_guid, error_code=DUPLICATE_EHR_ERROR)
        .build()
    )


def unacknowledged_duplicate_with_copcs_and_waiting_for_integration() -> List[Message]:
    """
    In this example, the sender returned two copies of the EHR,
    one of which the requester rejected as a duplicate.
    The sender also sent two COPC fragments, one of which was acknowledged by the requester.
    The requester is yet to integrate the record.
    """
    conversation_id = a_string()
    ehr_guid = a_string()
    duplicate_ehr_guid = a_string()
    fragment1_guid = a_string()
    fragment2_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid)
        .with_core_ehr(guid=duplicate_ehr_guid)
        .with_copc_fragment_continue()
        .with_copc_fragment(guid=fragment1_guid)
        .with_copc_fragment(guid=fragment2_guid)
        .with_requester_acknowledgement(message_ref=fragment1_guid)
        .with_requester_acknowledgement(
            message_ref=duplicate_ehr_guid, error_code=DUPLICATE_EHR_ERROR
        )
        .build()
    )


def ehr_integrated_successfully(**kwargs) -> List[Message]:
    """
    In this scenario, the GP2GP transfer was integrated successfully, no COPC messaging was used.
    """
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", req_complete_time + timedelta(days=1))
    conversation_id = a_string()
    ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_requester_acknowledgement(time=ehr_ack_time, message_ref=ehr_guid)
        .build()
    )


def ehr_integrated_late(**kwargs) -> List[Message]:
    """
    This scenario is an example of a GP2GP transfer where the records was integrated,
    but after more that 8 days.
    """
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", req_complete_time + timedelta(days=9))
    conversation_id = a_string()
    ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_requester_acknowledgement(time=ehr_ack_time, message_ref=ehr_guid)
        .build()
    )


def ehr_suppressed(**kwargs) -> List[Message]:
    """
    This scenario is an example of a GP2GP transfer where the records was integrated,
    but by being "suppressed".
    """
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", req_complete_time + timedelta(days=1))
    conversation_id = a_string()
    ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_requester_acknowledgement(
            time=ehr_ack_time, message_ref=ehr_guid, error_code=SUPPRESSED_EHR_ERROR
        )
        .build()
    )


def ehr_integration_failed(**kwargs) -> List[Message]:
    """
    This scenario is an example of a GP2GP transfer that failed to integrate.
    """
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", 28)
    conversation_id = a_string()
    ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_requester_acknowledgement(
            time=ehr_ack_time, message_ref=ehr_guid, error_code=ehr_ack_error
        )
        .build()
    )


def ehr_missing_message_for_an_acknowledgement() -> List[Message]:
    """
    In this scenario the sender acknowledged sent an acknowledgement to a non existent message.
    """
    conversation_id = a_string()
    message_ref_acknowledgement_to_non_existent_message = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(
            message_ref=message_ref_acknowledgement_to_non_existent_message
        )
        .build()
    )


def ehr_integrated_after_duplicate(**kwargs) -> List[Message]:
    """
    While normally the "request completed" message (aka "core ehr" message)
    is only transmitted once, sometimes there are duplicate copies sent by
    the sending practice. In this instance, the requester reported via negative
    ack that the second was a duplicate, then successfully integrated the first copy.
    """
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    conversation_id = a_string()
    ehr_guid = a_string()
    duplicate_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_core_ehr(guid=duplicate_ehr_guid)
        .with_requester_acknowledgement(
            message_ref=duplicate_ehr_guid, error_code=DUPLICATE_EHR_ERROR
        )
        .with_requester_acknowledgement(time=ehr_ack_time, message_ref=ehr_guid)
        .build()
    )


def integration_failed_after_duplicate(**kwargs) -> List[Message]:
    """
    While normally the "request completed" message (aka "core ehr" message)
    is only transmitted once, sometimes there are duplicate copies sent by
    the sending practice. In this instance, the requester reported via negative
    ack that the second was a duplicate, then failed to integrate the first copy.
    """
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", 11)
    conversation_id = a_string()
    ehr_guid = a_string()
    duplicate_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_core_ehr(guid=duplicate_ehr_guid)
        .with_requester_acknowledgement(
            message_ref=duplicate_ehr_guid, error_code=DUPLICATE_EHR_ERROR
        )
        .with_requester_acknowledgement(
            time=ehr_ack_time, message_ref=ehr_guid, error_code=ehr_ack_error
        )
        .build()
    )


def first_ehr_integrated_after_second_ehr_failed(**kwargs) -> List[Message]:
    """
    While normally the "request completed" message (aka "core ehr" message)
    is only transmitted once, sometimes there are duplicate copies sent by
    the sending practice. In this instance, the requester sent a negative
    acknowledgment in response to the second copy, then integrated the first.
    """
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", 11)
    conversation_id = a_string()
    first_ehr_guid = a_string()
    second_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=first_ehr_guid, time=req_complete_time)
        .with_core_ehr(guid=second_ehr_guid)
        .with_requester_acknowledgement(message_ref=second_ehr_guid, error_code=ehr_ack_error)
        .with_requester_acknowledgement(message_ref=first_ehr_guid, time=ehr_ack_time)
        .build()
    )


def first_ehr_integrated_before_second_ehr_failed(**kwargs) -> List[Message]:
    """
    While normally the "request completed" message (aka "core ehr" message)
    is only transmitted once, sometimes there are duplicate copies sent by
    the sending practice. In this instance, the requester integrated the
    first copy, then sent a negative acknowledgment in response to the second.
    """
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", 11)
    conversation_id = a_string()
    first_ehr_guid = a_string()
    second_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=first_ehr_guid, time=req_complete_time)
        .with_core_ehr(guid=second_ehr_guid)
        .with_requester_acknowledgement(message_ref=first_ehr_guid, time=ehr_ack_time)
        .with_requester_acknowledgement(message_ref=second_ehr_guid, error_code=ehr_ack_error)
        .build()
    )


def second_ehr_integrated_after_first_ehr_failed(**kwargs) -> List[Message]:
    """
    While normally the "request completed" message (aka "core ehr" message)
    is only transmitted once, sometimes there are duplicate copies sent by
    the sending practice. In this instance, the requester sent a negative
    acknowledgement in response to the first, then integrated the second copy,
    """
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", 11)
    conversation_id = a_string()
    first_ehr_guid = a_string()
    second_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=first_ehr_guid)
        .with_core_ehr(guid=second_ehr_guid, time=req_complete_time)
        .with_requester_acknowledgement(message_ref=first_ehr_guid, error_code=ehr_ack_error)
        .with_requester_acknowledgement(message_ref=second_ehr_guid, time=ehr_ack_time)
        .build()
    )


def second_ehr_integrated_before_first_ehr_failed(**kwargs) -> List[Message]:
    """
    While normally the "request completed" message (aka "core ehr" message)
    is only transmitted once, sometimes there are duplicate copies sent by
    the sending practice. In this instance, the requester integrated the
    second copy, and then sent a negative acknowledgement in response to the first.
    """
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", 11)
    conversation_id = a_string()
    first_ehr_guid = a_string()
    second_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=first_ehr_guid)
        .with_core_ehr(guid=second_ehr_guid, time=req_complete_time)
        .with_requester_acknowledgement(message_ref=second_ehr_guid, time=ehr_ack_time)
        .with_requester_acknowledgement(message_ref=first_ehr_guid, error_code=ehr_ack_error)
        .build()
    )


def ehr_integrated_with_duplicate_having_second_sender_ack_after_integration(
    **kwargs,
) -> List[Message]:
    """
    While normally the "request completed" message (aka "core ehr" message)
    is only transmitted once, sometimes there are duplicate copies sent by
    the sending practice. In this instance, the sender first sent two copies
    and the requester reported via negative ack that the first was a duplicate
    and that the second was integrated. After this the sender sent a third copy,
    which was ignored.
    """
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    req_complete_time_duplicate = req_complete_time - timedelta(hours=1)
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", req_complete_time)
    sender_ack_time = ehr_ack_time - timedelta(hours=1)
    request_complete_time_after_integration = ehr_ack_time + timedelta(hours=1)
    conversation_id = a_string()
    ehr_guid = a_string()
    duplicate_ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id, time=sender_ack_time)
        .with_core_ehr(guid=duplicate_ehr_guid, time=req_complete_time_duplicate)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_requester_acknowledgement(
            message_ref=duplicate_ehr_guid, error_code=DUPLICATE_EHR_ERROR
        )
        .with_requester_acknowledgement(time=ehr_ack_time, message_ref=ehr_guid)
        .with_core_ehr(guid=duplicate_ehr_guid, time=request_complete_time_after_integration)
        .build()
    )


def multiple_integration_failures(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where the sender returned multiple CORE EHRs,
    each of which the requester replied to with an error message.
    """
    error_codes = kwargs.get("error_codes", [99, 28, 21])

    ehr_guids = [a_string() for _ in range(len(error_codes))]
    conversation_id = a_string()

    test_case = (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
    )

    for guid in ehr_guids:
        test_case = test_case.with_core_ehr(guid=guid)

    for error_code, guid in zip(error_codes, ehr_guids):
        test_case = test_case.with_requester_acknowledgement(
            message_ref=guid, error_code=error_code
        )

    return test_case.build()


def copc_continue_sent(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where the record was large enough (or had enough attachments)
    to necessitate using COPC messages to transmit the data in multiple chunks.
    In this instance, the requester has sent a "continue" message telling the sender
    to begin transmission of COPC messages, however, the sender has yet to begin doing so.
    """
    conversation_id = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .with_copc_fragment_continue()
        .build()
    )


def copc_fragment_failure(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where the record was large enough (or had enough attachments)
    to necessitate using COPC messages to transmit the data in multiple chunks.
    In this instance, the sender has returned one COPC fragment,
    which the requester has replied to with a negative acknowledgement.
    """
    conversation_id = a_string()
    fragment_guid = a_string()
    fragment_error = kwargs.get("error_code", 30)
    copc_fragment_time = kwargs.get("copc_fragment_time", a_datetime())

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(
            message_ref=conversation_id, time=copc_fragment_time - timedelta(hours=2)
        )
        .with_core_ehr(time=copc_fragment_time - timedelta(hours=1))
        .with_copc_fragment_continue()
        .with_copc_fragment(guid=fragment_guid, time=copc_fragment_time)
        .with_requester_acknowledgement(message_ref=fragment_guid, error_code=fragment_error)
        .build()
    )


def copc_fragment_failure_and_missing_copc_fragment_ack(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where the record was large enough (or had enough attachments)
    to necessitate using COPC messages to transmit the data in multiple chunks.
    In this instance, the sender has returned two COPC fragments,
    one of which the requester has replied to with a negative acknowledgement,
    and the other of which is yet to be acknowledged.
    """
    conversation_id = a_string()
    fragment_guid = a_string()
    fragment_error = kwargs.get("error_code", 30)

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .with_copc_fragment_continue()
        .with_copc_fragment(guid=fragment_guid)
        .with_requester_acknowledgement(message_ref=fragment_guid, error_code=fragment_error)
        .with_copc_fragment()
        .build()
    )


def successful_integration_with_copc_fragments(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where the record was large enough (or had enough attachments)
    to necessitate using COPC messages to transmit the data in multiple chunks.
    In this instance, all transmitted COPC messages have been acknowledged by the requester,
    and the record has been integrated.
    """
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", req_complete_time + timedelta(days=1))
    conversation_id = a_string()
    ehr_guid = a_string()
    fragment1_guid = a_string()
    fragment2_guid = a_string()
    fragment3_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
        .with_copc_fragment_continue()
        .with_copc_fragment(guid=fragment1_guid)
        .with_copc_fragment(guid=fragment2_guid)
        .with_requester_acknowledgement(message_ref=fragment1_guid)
        .with_requester_acknowledgement(message_ref=fragment2_guid)
        .with_copc_fragment(guid=fragment3_guid)
        .with_requester_acknowledgement(message_ref=fragment3_guid)
        .with_requester_acknowledgement(message_ref=ehr_guid, time=ehr_ack_time)
        .build()
    )


def pending_integration_with_copc_fragments(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where the record was large enough (or had enough attachments)
    to necessitate using COPC messages to transmit the data in multiple chunks.
    In this instance, several COPC messages have been sent by the sender,
    but none have yet been acknowledged by the requester,
    """
    conversation_id = a_string()
    ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid)
        .with_copc_fragment_continue()
        .with_copc_fragment()
        .with_copc_fragment()
        .with_copc_fragment()
        .build()
    )


def pending_integration_with_acked_copc_fragments(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where the record was large enough (or had enough attachments)
    to necessitate using COPC messages to transmit the data in multiple chunks.
    In this instance, all transmitted COPC messages have been acknowledged by the requester,
    but the record is yet to be integrated.
    """
    conversation_id = a_string()
    ehr_guid = a_string()
    fragment1_guid = a_string()
    fragment2_guid = a_string()
    fragment3_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid)
        .with_copc_fragment_continue()
        .with_copc_fragment(guid=fragment1_guid)
        .with_copc_fragment(guid=fragment2_guid)
        .with_requester_acknowledgement(message_ref=fragment1_guid)
        .with_requester_acknowledgement(message_ref=fragment2_guid)
        .with_copc_fragment(guid=fragment3_guid)
        .with_requester_acknowledgement(message_ref=fragment3_guid)
        .build()
    )


def copc_fragment_failures(**kwargs) -> List[Message]:
    """
    A GP2GP transfer where some number of COPC fragments returned by the sender are
    negatively acknowledged by the requester with error codes.
    """
    copc_error_codes = [20, 29, 30]
    error_codes = kwargs.get("error_codes", copc_error_codes)
    conversation_id = a_string()
    fragment_guids = [a_string() for _ in range(len(error_codes))]

    test_case = (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .with_copc_fragment_continue()
    )

    for guid in fragment_guids:
        test_case = test_case.with_copc_fragment(guid=guid)

    for error_code, guid in zip(error_codes, fragment_guids):
        test_case = test_case.with_requester_acknowledgement(
            message_ref=guid, error_code=error_code
        )

    return test_case.build()


def _concluded_with_conflicting_acks_and_duplicate_ehrs(**kwargs):
    conversation_id = a_string()
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", req_complete_time + timedelta(hours=4))
    ehr_ack_code = kwargs.get("ehr_ack_code", None)

    ehr_guid_1 = a_string()
    ehr_guid_2 = a_string()
    ehr_guid_3 = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid_1)
        .with_core_ehr(guid=ehr_guid_2, time=req_complete_time)
        .with_core_ehr(guid=ehr_guid_3)
        .with_requester_acknowledgement(message_ref=ehr_guid_2, error_code=DUPLICATE_EHR_ERROR)
        .with_requester_acknowledgement(
            message_ref=ehr_guid_2, error_code=ehr_ack_code, time=ehr_ack_time
        )
        .with_requester_acknowledgement(message_ref=ehr_guid_1, error_code=DUPLICATE_EHR_ERROR)
        .build()
    )


def ehr_integrated_with_conflicting_acks_and_duplicate_ehrs(**kwargs) -> List[Message]:
    """
    A multi edge case where the sender returned three EHRs,
    the first reviving a duplicate ack, the second receiving both a duplicate and a positive ack,
    and the third receiving no ack at all.
    """
    return _concluded_with_conflicting_acks_and_duplicate_ehrs(ehr_ack_code=None, **kwargs)


def ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs(**kwargs) -> List[Message]:
    """
    A multi edge case where the sender returned three EHRs,
    the first reviving a duplicate ack, the second receiving both a duplicate and a suppressed ack,
    and the third receiving no ack at all.
    """
    return _concluded_with_conflicting_acks_and_duplicate_ehrs(
        ehr_ack_code=SUPPRESSED_EHR_ERROR, **kwargs
    )


def integration_failed_with_conflicting_acks_and_duplicate_ehrs(**kwargs) -> List[Message]:
    """
    A multi edge case where the sender returned three EHRs,
    the first reviving a duplicate ack, the second receiving both a duplicate and a negative ack,
    and the third receiving no ack at all.
    """
    ehr_ack_error = kwargs.get("error_code", 11)
    return _concluded_with_conflicting_acks_and_duplicate_ehrs(ehr_ack_code=ehr_ack_error, **kwargs)


def _concluded_with_conflicting_acks(**kwargs) -> List[Message]:
    conversation_id = a_string()
    default_codes_and_times = [(None, a_datetime()), (DUPLICATE_EHR_ERROR, a_datetime())]
    codes_and_times = kwargs.get("codes_and_times", default_codes_and_times)
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_guid = a_string()

    test_case = (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid, time=req_complete_time)
    )

    for code, ack_time in codes_and_times:
        test_case = test_case.with_requester_acknowledgement(
            message_ref=ehr_guid,
            error_code=code,
            time=ack_time,
        )

    return test_case.build()


def ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack(**kwargs) -> List[Message]:
    """
    Usually a requesting system acknowledges the GP2GP request completed message
    (aka core EHR) once. However, in this case there are three conflicting acknowledgements
    to the Core EHR message, with the requester reporting an error code, a duplicate,
    and an integrated record.
    """
    request_completed_time = kwargs.setdefault("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", request_completed_time + timedelta(hours=4))

    return _concluded_with_conflicting_acks(
        **kwargs,
        codes_and_times=[
            (11, a_datetime()),
            (DUPLICATE_EHR_ERROR, a_datetime()),
            (None, ehr_ack_time),
        ],
    )


def ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack(**kwargs) -> List[Message]:
    """
    Usually a requesting system acknowledges the GP2GP request completed message
    (aka core EHR) once. However, in this case there are three conflicting acknowledgements
    to the Core EHR message, with the requester reporting an error code, a duplicate,
    and a suppressed record.
    """
    request_completed_time = kwargs.setdefault("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", request_completed_time + timedelta(hours=4))

    return _concluded_with_conflicting_acks(
        **kwargs,
        codes_and_times=[
            (11, a_datetime()),
            (DUPLICATE_EHR_ERROR, a_datetime()),
            (SUPPRESSED_EHR_ERROR, ehr_ack_time),
        ],
    )


def multiple_sender_acknowledgements(**kwargs) -> List[Message]:
    """
    The GP2GP request is acknowledged by the sender, not with the usual single acknowledgement,
    but twice by two separate messages.
    """
    error_codes = kwargs.get("error_codes", [None] * an_integer(2, 4))

    conversation_id = a_string()

    test_case = GP2GPTestCase(conversation_id=conversation_id).with_request()

    for code in error_codes:
        test_case = test_case.with_sender_acknowledgement(
            message_ref=conversation_id, error_code=code
        )

    return test_case.build()
