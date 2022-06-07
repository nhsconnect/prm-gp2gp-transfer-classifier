from prmdata.domain.mi.mi_service import MiMessage, MiService
from tests.builders.common import a_datetime, a_string


def test_construct_mi_messages_from_mi_events():
    a_conversation_id = "1111-1111-1111-1111"
    an_event_id = "1234-5678-8765-4321"
    a_random_datetime = a_datetime()
    a_supplier = a_string()
    an_ods_code = a_string()
    another_datetime = a_datetime()

    mi_events = [
        {
            "eventId": an_event_id,
            "conversationId": a_conversation_id,
            "eventGeneratedDateTime": a_random_datetime,
            "reportingSystemSupplier": a_supplier,
            "reportingPracticeOdsCode": an_ods_code,
            "transferEventDateTime": another_datetime,
        }
    ]

    expected = [
        MiMessage(
            conversation_id=a_conversation_id,
            event_id=an_event_id,
            event_generated_datetime=a_random_datetime,
            reporting_system_supplier=a_supplier,
            reporting_practice_ods_code=an_ods_code,
            transfer_event_datetime=another_datetime,
        )
    ]

    actual = MiService.construct_mi_messages_from_mi_events(mi_events)

    assert actual == expected
