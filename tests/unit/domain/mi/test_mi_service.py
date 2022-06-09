from prmdata.domain.mi.mi_service import (
    MiMessage,
    MiMessagePayload,
    MiMessagePayloadIntegration,
    MiMessagePayloadRegistration,
    MiService,
)
from tests.builders.common import a_datetime, a_string
from tests.builders.mi_message import build_mi_message


def test_construct_mi_messages_from_mi_events():
    a_conversation_id = "1111-1111-1111-1111"
    an_event_id = "1234-5678-8765-4321"
    an_event_type = "REGISTRATION_STARTED"
    a_protocol = "PRE_TRANSFER"
    a_random_datetime = a_datetime()
    a_supplier = a_string()
    an_ods_code = a_string()
    another_ods_code = a_string()
    another_datetime = a_datetime()
    a_registration_type = "newRegistrant"
    a_status = "MERGED"
    a_reason = "reason"

    mi_events = [
        {
            "conversationId": a_conversation_id,
            "eventId": an_event_id,
            "eventType": an_event_type,
            "transferProtocol": a_protocol,
            "eventGeneratedDateTime": a_random_datetime,
            "reportingSystemSupplier": a_supplier,
            "reportingPracticeOdsCode": an_ods_code,
            "transferEventDateTime": another_datetime,
            "payload": {
                "registration": {
                    "registrationStartedDateTime": a_random_datetime,
                    "registrationType": a_registration_type,
                    "requestingPracticeOdsCode": an_ods_code,
                    "sendingPracticeOdsCode": another_ods_code,
                },
                "integration": {"integrationStatus": a_status, "reason": a_reason},
            },
        }
    ]

    expected = [
        MiMessage(
            conversation_id=a_conversation_id,
            event_id=an_event_id,
            event_type=an_event_type,
            transfer_protocol=a_protocol,
            event_generated_datetime=a_random_datetime,
            reporting_system_supplier=a_supplier,
            reporting_practice_ods_code=an_ods_code,
            transfer_event_datetime=another_datetime,
            payload=MiMessagePayload(
                registration=MiMessagePayloadRegistration(
                    registrationStartedDateTime=a_random_datetime,
                    registrationType=a_registration_type,
                    requestingPracticeOdsCode=an_ods_code,
                    sendingPracticeOdsCode=another_ods_code,
                ),
                integration=MiMessagePayloadIntegration(
                    integrationStatus=a_status, reason=a_reason
                ),
            ),
        )
    ]

    actual = MiService.construct_mi_messages_from_mi_events(mi_events)

    assert actual == expected


def test_handles_missing_fields_when_construct_mi_messages_from_mi_events():
    a_conversation_id = "1111-1111-1111-1111"
    an_event_id = "1234-5678-8765-4321"
    an_event_type = "REGISTRATION_STARTED"
    a_protocol = "PRE_TRANSFER"
    a_random_datetime = a_datetime()
    a_supplier = a_string()
    an_ods_code = a_string()
    another_datetime = a_datetime()

    mi_events = [
        {
            "conversationId": a_conversation_id,
            "eventId": an_event_id,
            "eventType": an_event_type,
            "transferProtocol": a_protocol,
            "eventGeneratedDateTime": a_random_datetime,
            "reportingSystemSupplier": a_supplier,
            "reportingPracticeOdsCode": an_ods_code,
            "transferEventDateTime": another_datetime,
            "payload": {"registration": {}, "integration": {}},
        }
    ]

    expected = [
        MiMessage(
            conversation_id=a_conversation_id,
            event_id=an_event_id,
            event_type=an_event_type,
            transfer_protocol=a_protocol,
            event_generated_datetime=a_random_datetime,
            reporting_system_supplier=a_supplier,
            reporting_practice_ods_code=an_ods_code,
            transfer_event_datetime=another_datetime,
            payload=MiMessagePayload(
                registration=MiMessagePayloadRegistration(
                    registrationStartedDateTime=None,
                    registrationType=None,
                    requestingPracticeOdsCode=None,
                    sendingPracticeOdsCode=None,
                ),
                integration=MiMessagePayloadIntegration(integrationStatus=None, reason=None),
            ),
        )
    ]

    actual = MiService.construct_mi_messages_from_mi_events(mi_events)

    assert actual == expected


def test_group_mi_messages_by_conversation_id():
    conversation_id_one = "1111-1111-1111-1111"
    conversation_id_two = "2222-2222-2222-2222"
    conversation_id_three = "3333-3333-3333-3333"

    mi_message_one = build_mi_message(conversation_id=conversation_id_one)
    mi_message_two = build_mi_message(conversation_id=conversation_id_two)
    mi_message_three = build_mi_message(conversation_id=conversation_id_one)
    mi_message_four = build_mi_message(conversation_id=conversation_id_three)

    mi_events = [mi_message_one, mi_message_two, mi_message_three, mi_message_four]

    expected = {
        conversation_id_one: [mi_message_one, mi_message_three],
        conversation_id_two: [mi_message_two],
        conversation_id_three: [mi_message_four],
    }

    actual = MiService.group_mi_messages_by_conversation_id(mi_events)

    assert actual == expected
