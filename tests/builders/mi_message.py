from prmdata.domain.mi.mi_message import (
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadIntegration,
    MiMessagePayloadRegistration,
)
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
        payload=kwargs.get("payload", build_mi_message_payload(**kwargs)),
    )


def build_mi_message_payload(**kwargs) -> MiMessagePayload:
    return MiMessagePayload(
        registration=kwargs.get("registration", build_mi_message_payload_registration(**kwargs)),
        integration=kwargs.get("integration", build_mi_message_payload_integration(**kwargs)),
        ehr=None,
        transfer_compatibility_status=None,
        demographic_trace_status=None,
        smartcard_present=None,
        structured_record_migration=None,
        document_migration=None,
    )


def build_mi_message_payload_registration(**kwargs) -> MiMessagePayloadRegistration:
    return MiMessagePayloadRegistration(
        registration_type=kwargs.get("registration_type", a_string()),
        requesting_practice_ods_code=kwargs.get("requesting_practice_ods_code", a_string()),
        sending_practice_ods_code=kwargs.get("sending_practice_ods_code", a_string()),
    )


def build_mi_message_payload_integration(**kwargs) -> MiMessagePayloadIntegration:
    return MiMessagePayloadIntegration(
        integration_status=kwargs.get("integration_status", a_string()),
        reason=kwargs.get("reason", a_string()),
    )
