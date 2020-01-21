from datetime import datetime

from gp2gp.models.spine import Message


A_DATETIME = datetime(year=2020, month=6, day=6)
A_GUID = "C80F9DC0-10F5-11EA-A8DF-A33D4E28615A"
AN_INTERACTION_ID = "COPC_IN000001UK01"
AN_ODS_CODE = "P82023"


def build_message(conversation_id=None, time=None):
    return Message(
        time=A_DATETIME if time is None else time,
        conversation_id=A_GUID if conversation_id is None else conversation_id,
        guid=A_GUID,
        interaction_id=AN_INTERACTION_ID,
        from_party_ods=AN_ODS_CODE,
        to_party_ods=AN_ODS_CODE,
        message_ref=A_GUID,
        error_code=None,
    )
