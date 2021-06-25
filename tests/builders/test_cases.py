from prmdata.domain.spine.message import (
    Message,
    EHR_REQUEST_STARTED,
    APPLICATION_ACK,
    EHR_REQUEST_COMPLETED,
)
from tests.builders.common import a_datetime, a_string, a_duration

DUPLICATE_EHR_ERROR = 12


def request_made(**kwargs):
    gp2gp_request = Message(
        time=kwargs.get("request_sent_date", a_datetime()),
        conversation_id=a_string(),
        guid=a_string(),
        interaction_id=EHR_REQUEST_STARTED,
        from_party_asid=kwargs.get("requesting_asid", a_string()),
        to_party_asid=kwargs.get("sending_asid", a_string()),
        message_ref=None,
        error_code=None,
        from_system=None,
        to_system=None,
    )

    return [gp2gp_request]


def request_acknowledged_successfully():
    conversation_id = a_string()
    requesting_asid = a_string()
    sending_asid = a_string()

    gp2gp_request = Message(
        time=a_datetime(),
        conversation_id=conversation_id,
        guid=conversation_id,
        interaction_id=EHR_REQUEST_STARTED,
        from_party_asid=requesting_asid,
        to_party_asid=sending_asid,
        message_ref=None,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    gp2gp_request_acknowledgement = Message(
        time=gp2gp_request.time + a_duration(),
        conversation_id=conversation_id,
        guid=a_string(),
        interaction_id=APPLICATION_ACK,
        from_party_asid=sending_asid,
        to_party_asid=requesting_asid,
        message_ref=gp2gp_request.guid,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    return [gp2gp_request, gp2gp_request_acknowledgement]


def core_ehr_sent():
    conversation_id = a_string()
    requesting_asid = a_string()
    sending_asid = a_string()

    gp2gp_request = Message(
        time=a_datetime(),
        conversation_id=conversation_id,
        guid=conversation_id,
        interaction_id=EHR_REQUEST_STARTED,
        from_party_asid=requesting_asid,
        to_party_asid=sending_asid,
        message_ref=None,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    gp2gp_request_acknowledgement = Message(
        time=gp2gp_request.time + a_duration(),
        conversation_id=conversation_id,
        guid=a_string(),
        interaction_id=APPLICATION_ACK,
        from_party_asid=sending_asid,
        to_party_asid=requesting_asid,
        message_ref=gp2gp_request.guid,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    core_ehr = Message(
        time=gp2gp_request_acknowledgement.time + a_duration(),
        conversation_id=conversation_id,
        guid=a_string(),
        interaction_id=EHR_REQUEST_COMPLETED,
        from_party_asid=sending_asid,
        to_party_asid=requesting_asid,
        message_ref=None,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    return [gp2gp_request, gp2gp_request_acknowledgement, core_ehr]


def acknowledged_duplicate_ehr_and_waiting_for_integration():
    conversation_id = a_string()
    requesting_asid = a_string()
    sending_asid = a_string()

    gp2gp_request = Message(
        time=a_datetime(),
        conversation_id=conversation_id,
        guid=conversation_id,
        interaction_id=EHR_REQUEST_STARTED,
        from_party_asid=requesting_asid,
        to_party_asid=sending_asid,
        message_ref=None,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    gp2gp_request_acknowledgement = Message(
        time=gp2gp_request.time + a_duration(),
        conversation_id=conversation_id,
        guid=a_string(),
        interaction_id=APPLICATION_ACK,
        from_party_asid=sending_asid,
        to_party_asid=requesting_asid,
        message_ref=gp2gp_request.guid,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    core_ehr = Message(
        time=gp2gp_request_acknowledgement.time + a_duration(),
        conversation_id=conversation_id,
        guid=a_string(),
        interaction_id=EHR_REQUEST_COMPLETED,
        from_party_asid=sending_asid,
        to_party_asid=requesting_asid,
        message_ref=None,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    duplicate_core_ehr = Message(
        time=core_ehr.time + a_duration(),
        conversation_id=conversation_id,
        guid=a_string(),
        interaction_id=EHR_REQUEST_COMPLETED,
        from_party_asid=sending_asid,
        to_party_asid=requesting_asid,
        message_ref=None,
        error_code=None,
        from_system=None,
        to_system=None,
    )
    duplicate_core_ehr_acknowledgement = Message(
        time=duplicate_core_ehr.time + a_duration(),
        conversation_id=conversation_id,
        guid=a_string(),
        interaction_id=APPLICATION_ACK,
        from_party_asid=requesting_asid,
        to_party_asid=sending_asid,
        message_ref=duplicate_core_ehr.guid,
        error_code=DUPLICATE_EHR_ERROR,
        from_system=None,
        to_system=None,
    )
    return [
        gp2gp_request,
        gp2gp_request_acknowledgement,
        core_ehr,
        duplicate_core_ehr,
        duplicate_core_ehr_acknowledgement,
    ]
