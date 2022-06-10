from prmdata.domain.mi.mi_service import (
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
    )


def build_mi_message_payload_registration(**kwargs) -> MiMessagePayloadRegistration:
    return MiMessagePayloadRegistration(
        registrationStartedDateTime=kwargs.get("registrationStartedDateTime", a_datetime()),
        registrationType=kwargs.get("registrationType", a_string()),
        requestingPracticeOdsCode=kwargs.get("requestingPracticeOdsCode", a_string()),
        sendingPracticeOdsCode=kwargs.get("sendingPracticeOdsCode", a_string()),
    )


def build_mi_message_payload_integration(**kwargs) -> MiMessagePayloadIntegration:
    return MiMessagePayloadIntegration(
        integrationStatus=kwargs.get("integrationStatus", a_string()),
        reason=kwargs.get("reason", a_string()),
    )
