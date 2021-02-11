from datetime import timedelta, datetime

from gp2gp.service.models import TransferStatus
from gp2gp.service.transformers import convert_transfers_to_dict

from tests.builders.service import build_transfer


def test_converts_complete_transfer_to_dict():
    conversation_id = "efnewife-ewfewfef"
    requesting_practice_asid = "121212121212"
    sending_practice_asid = "221212121212"
    sla_duration = timedelta(seconds=5000)
    date_requested = datetime(2020, 7, 23)
    date_completed = datetime(2020, 7, 24)
    transfers = [
        build_transfer(
            conversation_id=conversation_id,
            date_requested=date_requested,
            date_completed=date_completed,
            requesting_practice_asid=requesting_practice_asid,
            sending_practice_asid=sending_practice_asid,
            sla_duration=sla_duration,
            status=TransferStatus.INTEGRATED,
        )
    ]

    expected = [
        {
            "conversationId": conversation_id,
            "dateCompleted": date_completed.isoformat(),
            "dateRequested": date_requested.isoformat(),
            "finalErrorCode": None,
            "intermediateErrorCodes": [],
            "requestingPracticeAsid": requesting_practice_asid,
            "sendingPracticeAsid": sending_practice_asid,
            "slaDuration": sla_duration.seconds,
            "status": "INTEGRATED",
        }
    ]

    actual = convert_transfers_to_dict(transfers)

    assert actual == expected


def test_converts_failed_transfer_to_dict():
    conversation_id = "efnewife-ewfewfef"
    requesting_practice_asid = "121212121212"
    sending_practice_asid = "221212121212"
    date_requested = datetime(2020, 7, 23)
    transfers = [
        build_transfer(
            conversation_id=conversation_id,
            date_requested=date_requested,
            date_completed=None,
            requesting_practice_asid=requesting_practice_asid,
            sending_practice_asid=sending_practice_asid,
            sla_duration=None,
            intermediate_error_codes=[5],
            status=TransferStatus.FAILED,
        )
    ]

    expected = [
        {
            "conversationId": conversation_id,
            "dateCompleted": None,
            "dateRequested": date_requested.isoformat(),
            "finalErrorCode": None,
            "intermediateErrorCodes": [5],
            "requestingPracticeAsid": requesting_practice_asid,
            "sendingPracticeAsid": sending_practice_asid,
            "slaDuration": None,
            "status": "FAILED",
        }
    ]

    actual = convert_transfers_to_dict(transfers)

    assert actual == expected
