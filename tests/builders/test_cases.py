from prmdata.domain.spine.message import (
    Message,
    EHR_REQUEST_STARTED,
    APPLICATION_ACK,
    EHR_REQUEST_COMPLETED,
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


def request_made(**kwargs):
    request_time = kwargs.get("request_sent_date", a_datetime())
    requester_asid = kwargs.get("requesting_asid", a_string())
    sender_asid = kwargs.get("sending_asid", a_string())

    return (
        GP2GPTestCase(requesting_asid=requester_asid, sending_asid=sender_asid)
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
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
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


def suppressed_ehr(**kwargs):
    ehr_ack_time = kwargs.get("ehr_acknowledge_time", a_datetime())
    req_complete_time = kwargs.get("request_completed_time", a_datetime())
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


def concluded_with_failure(**kwargs):
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


def multiple_failures(**kwargs):
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
