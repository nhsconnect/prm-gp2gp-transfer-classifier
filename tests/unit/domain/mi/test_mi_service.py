from prmdata.domain.mi.mi_service import MiMessage, MiService


def test_construct_messages_from_mi_events():
    a_conversation_id = "1111-1111-1111-1111"
    an_event_id = "1234-5678-8765-4321"
    mi_events = [{"eventId": an_event_id, "conversationId": a_conversation_id}]

    expected = [MiMessage(conversation_id=a_conversation_id, event_id=an_event_id)]

    actual = MiService.construct_mi_messages_from_mi_events(mi_events)

    assert actual == expected
