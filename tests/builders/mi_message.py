from prmdata.domain.mi.mi_service import MiMessage
from tests.builders.common import a_datetime, a_string


def build_mi_message(**kwargs) -> MiMessage:
    return MiMessage(
        conversation_id=kwargs.get("conversation_id", a_string(16)),
        event_id=kwargs.get("event_id", a_string(16)),
        event_type=kwargs.get("event_type", a_string(8)),
        transfer_protocol=kwargs.get("transfer_protocol", a_string(8)),
        event_generated_datetime=kwargs.get("event_generated_datetime", a_datetime()),
        reporting_system_supplier=kwargs.get("reporting_system_supplier", a_string(8)),
        reporting_practice_ods_code=kwargs.get("reporting_practice_ods_code", a_string(8)),
        transfer_event_datetime=kwargs.get("transfer_event_datetime", a_datetime()),
    )
