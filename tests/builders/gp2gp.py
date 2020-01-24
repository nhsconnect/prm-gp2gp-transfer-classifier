from gp2gp.models.gp2gp import Transfer
from tests.builders.common import a_string, a_duration


def build_transfer(**kwargs):
    return Transfer(
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        sla_duration=kwargs.get("sla_duration", a_duration()),
        requesting_practice_ods=kwargs.get("requesting_practice_ods", a_string(6)),
        sending_practice_ods=kwargs.get("sending_practice_ods", a_string(6)),
        error_code=kwargs.get("error_code", None),
    )
