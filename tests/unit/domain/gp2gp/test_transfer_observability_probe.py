from unittest.mock import Mock

from prmdata.domain.gp2gp.transfer_service import TransferServiceObservabilityProbe
from tests.builders.common import a_datetime, a_string
from tests.builders.spine import build_mock_gp2gp_conversation


def test_probe_should_log_warning_given_negative_sla():
    conversation_id = a_string()
    final_acknowledgement_time = a_datetime(year=2021, month=11)
    request_completed_time = a_datetime(year=2021, month=12)
    conversation = build_mock_gp2gp_conversation(
        conversation_id=conversation_id,
        final_acknowledgement_time=final_acknowledgement_time,
        request_completed_time=request_completed_time,
    )
    mock_logger = Mock()

    probe = TransferServiceObservabilityProbe(logger=mock_logger)
    probe.record_negative_sla(conversation)

    mock_logger.warning.assert_called_once_with(
        f":Negative SLA duration for conversation: {conversation_id}",
        extra={
            "event": "NEGATIVE_SLA_DETECTED",
            "conversation_id": conversation_id,
            "final_acknowledgement_time": final_acknowledgement_time,
            "request_completed_time": request_completed_time,
        },
    )
