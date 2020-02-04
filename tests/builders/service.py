from gp2gp.service.models import PracticeSlaMetrics, Transfer
from tests.builders.common import a_duration, a_string, an_integer


def build_transfer(**kwargs):
    return Transfer(
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        sla_duration=kwargs.get("sla_duration", a_duration()),
        requesting_practice_ods=kwargs.get("requesting_practice_ods", a_string(6)),
        sending_practice_ods=kwargs.get("sending_practice_ods", a_string(6)),
        error_code=kwargs.get("error_code", None),
        pending=kwargs.get("pending", False),
    )


def build_practice_sla_metrics(**kwargs):
    return PracticeSlaMetrics(
        ods=kwargs.get("ods", a_string(6)),
        within_3_days=kwargs.get("within_3_days", an_integer()),
        within_8_days=kwargs.get("within_8_days", an_integer()),
        beyond_8_days=kwargs.get("beyond_8_days", an_integer()),
    )
