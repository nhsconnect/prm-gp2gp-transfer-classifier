from datetime import timedelta

from prmdata.domain.spine.message import (
    Message,
    EHR_REQUEST_STARTED,
    APPLICATION_ACK,
    EHR_REQUEST_COMPLETED,
    COMMON_POINT_TO_POINT,
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

    def build(self):
        return self._messages

    def with_large_fragment_continue(self, **kwargs):
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

    def with_large_fragment(self, **kwargs):
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


def request_made(**kwargs):
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


def request_acknowledged_successfully():
    conversation_id = a_string()
    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .build()
    )


def request_acknowledged_with_error(**kwargs):
    conversation_id = a_string()
    request_ack_error = kwargs.get("error_code", an_integer(a=31, b=98))
    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id, error_code=request_ack_error)
        .build()
    )


def core_ehr_sent():
    conversation_id = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .build()
    )


def acknowledged_duplicate_and_waiting_for_integration():
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


def ehr_integrated_successfully(**kwargs):
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


def ehr_integrated_late(**kwargs):
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


def ehr_suppressed(**kwargs):
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


def ehr_integration_failed(**kwargs):
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", an_integer(a=20, b=30))
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


def ehr_integrated_after_duplicate(**kwargs):
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


def integration_failed_after_duplicate(**kwargs):
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", an_integer(a=20, b=30))
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


def first_ehr_integrated_after_second_ehr_failed(**kwargs):
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", an_integer(a=20, b=30))
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


def first_ehr_integrated_before_second_ehr_failed(**kwargs):
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", an_integer(a=20, b=30))
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


def second_ehr_integrated_after_first_ehr_failed(**kwargs):
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", an_integer(a=20, b=30))
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


def second_ehr_integrated_before_first_ehr_failed(**kwargs):
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
    ehr_ack_error = kwargs.get("error_code", an_integer(a=20, b=30))
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


def _some_error_codes():
    return [an_integer(20, 30) for _ in range(an_integer(2, 4))]


def multiple_integration_failures(**kwargs):
    error_codes = kwargs.get("error_codes", _some_error_codes())

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


def large_message_continue_sent(**kwargs):
    conversation_id = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .with_large_fragment_continue()
        .build()
    )


def large_message_fragment_failure(**kwargs):
    conversation_id = a_string()
    fragment_guid = a_string()
    fragment_error = kwargs.get("error_code", an_integer(a=20, b=30))

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .with_large_fragment_continue()
        .with_large_fragment(guid=fragment_guid)
        .with_requester_acknowledgement(message_ref=fragment_guid, error_code=fragment_error)
        .build()
    )


def large_message_fragment_failure_and_missing_large_fragment_ack(**kwargs):
    conversation_id = a_string()
    fragment_guid = a_string()
    fragment_error = kwargs.get("error_code", an_integer(a=20, b=30))

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .with_large_fragment_continue()
        .with_large_fragment(guid=fragment_guid)
        .with_requester_acknowledgement(message_ref=fragment_guid, error_code=fragment_error)
        .with_large_fragment()
        .build()
    )


def successful_integration_with_large_messages(**kwargs):
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
        .with_large_fragment_continue()
        .with_large_fragment(guid=fragment1_guid)
        .with_large_fragment(guid=fragment2_guid)
        .with_requester_acknowledgement(message_ref=fragment1_guid)
        .with_requester_acknowledgement(message_ref=fragment2_guid)
        .with_large_fragment(guid=fragment3_guid)
        .with_requester_acknowledgement(message_ref=fragment3_guid)
        .with_requester_acknowledgement(message_ref=ehr_guid)
        .build()
    )


def pending_integration_with_large_message_fragments(**kwargs):
    conversation_id = a_string()
    ehr_guid = a_string()

    return (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr(guid=ehr_guid)
        .with_large_fragment_continue()
        .with_large_fragment()
        .with_large_fragment()
        .with_large_fragment()
        .build()
    )


def pending_integration_with_acked_large_message_fragments(**kwargs):
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
        .with_large_fragment_continue()
        .with_large_fragment(guid=fragment1_guid)
        .with_large_fragment(guid=fragment2_guid)
        .with_requester_acknowledgement(message_ref=fragment1_guid)
        .with_requester_acknowledgement(message_ref=fragment2_guid)
        .with_large_fragment(guid=fragment3_guid)
        .with_requester_acknowledgement(message_ref=fragment3_guid)
        .build()
    )


def multiple_large_fragment_failures(**kwargs):
    error_codes = kwargs.get("error_codes", _some_error_codes())
    conversation_id = a_string()
    fragment_guids = [a_string() for _ in range(len(error_codes))]

    test_case = (
        GP2GPTestCase(conversation_id=conversation_id)
        .with_request()
        .with_sender_acknowledgement(message_ref=conversation_id)
        .with_core_ehr()
        .with_large_fragment_continue()
    )

    for guid in fragment_guids:
        test_case = test_case.with_large_fragment(guid=guid)

    for error_code, guid in zip(error_codes, fragment_guids):
        test_case = test_case.with_requester_acknowledgement(
            message_ref=guid, error_code=error_code
        )

    return test_case.build()


def concluded_with_conflicting_acks_and_duplicate_ehrs(**kwargs):
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


def ehr_integrated_with_conflicting_acks_and_duplicate_ehrs(**kwargs):
    return concluded_with_conflicting_acks_and_duplicate_ehrs(ehr_ack_code=None, **kwargs)


def ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs(**kwargs):

    return concluded_with_conflicting_acks_and_duplicate_ehrs(
        ehr_ack_code=SUPPRESSED_EHR_ERROR, **kwargs
    )


def integration_failed_with_conflicting_acks_and_duplicate_ehrs(**kwargs):
    ehr_ack_error = kwargs.get("error_code", an_integer(a=20, b=30))
    return concluded_with_conflicting_acks_and_duplicate_ehrs(ehr_ack_code=ehr_ack_error, **kwargs)


def concluded_with_conflicting_acks(**kwargs):
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


def ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack(**kwargs):
    request_completed_time = kwargs.setdefault("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", request_completed_time + timedelta(hours=4))

    return concluded_with_conflicting_acks(
        **kwargs,
        codes_and_times=[
            (11, a_datetime()),
            (DUPLICATE_EHR_ERROR, a_datetime()),
            (None, ehr_ack_time),
        ],
    )


def ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack(**kwargs):
    request_completed_time = kwargs.setdefault("request_completed_time", a_datetime())
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", request_completed_time + timedelta(hours=4))

    return concluded_with_conflicting_acks(
        **kwargs,
        codes_and_times=[
            (11, a_datetime()),
            (DUPLICATE_EHR_ERROR, a_datetime()),
            (SUPPRESSED_EHR_ERROR, ehr_ack_time),
        ],
    )
