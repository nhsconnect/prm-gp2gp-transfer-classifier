from prmdata.domain.spine.message import Message, EHR_REQUEST_STARTED
from tests.builders.common import a_datetime, a_string


def gp2gp_request_made(**kwargs):
    return [
        Message(
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
        ),
    ]
