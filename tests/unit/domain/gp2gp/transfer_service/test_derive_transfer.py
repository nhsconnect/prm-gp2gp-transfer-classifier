from datetime import timedelta
from unittest.mock import Mock

from prmdata.domain.gp2gp.transfer_service import TransferService
from prmdata.domain.spine.gp2gp_conversation import Gp2gpConversation
from tests.builders import test_cases
from tests.builders.common import a_datetime, a_string
from tests.builders.spine import build_mock_gp2gp_conversation

mock_transfer_observability_probe = Mock()
mock_gp2gp_conversation_observability_probe = Mock()


def test_extracts_conversation_id():
    conversation = build_mock_gp2gp_conversation(conversation_id="1234")
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)
    expected_conversation_id = "1234"

    assert actual.conversation_id == expected_conversation_id


def test_produces_sla_of_successful_conversation():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=a_datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0),
        final_acknowledgement_time=a_datetime(
            year=2020, month=6, day=1, hour=13, minute=52, second=0
        ),
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration


def test_logs_negative_sla_warning():
    conversation_id = a_string()
    mock_probe = Mock()
    conversation = build_mock_gp2gp_conversation(
        conversation_id=conversation_id,
        final_acknowledgement_time=a_datetime(year=2021, month=12, day=1),
        request_completed_time=a_datetime(year=2021, month=12, day=2),
    )
    mock_lookup = Mock()

    transfer_service = TransferService(cutoff=timedelta(days=14), observability_probe=mock_probe)

    transfer_service.derive_transfer(conversation, mock_lookup)

    mock_probe.record_negative_sla.assert_called_once_with(conversation)


def test_logs_unknown_practice_for_transfer_warning_given_no_sending_practice_ods_code():
    conversation_id = a_string()
    mock_probe = Mock()
    conversation = build_mock_gp2gp_conversation(
        conversation_id=conversation_id, sending_practice_asid="314135442432"
    )
    mock_lookup = Mock()
    mock_lookup.has_asid_code.return_value = False

    transfer_service = TransferService(cutoff=timedelta(days=14), observability_probe=mock_probe)
    transfer_service.derive_transfer(conversation, mock_lookup)

    mock_probe.record_no_ods_code_for_asid.assert_any_call(conversation_id, "314135442432")


def test_logs_unknown_practice_for_transfer_warning_given_no_requesting_practice_ods_code():
    conversation_id = a_string()
    mock_probe = Mock()
    conversation = build_mock_gp2gp_conversation(
        conversation_id=conversation_id, requesting_practice_asid="12131413535"
    )
    mock_lookup = Mock()
    mock_lookup.has_asid_code.return_value = False

    transfer_service = TransferService(cutoff=timedelta(days=14), observability_probe=mock_probe)
    transfer_service.derive_transfer(conversation, mock_lookup)

    mock_probe.record_no_ods_code_for_asid.assert_any_call(conversation_id, "12131413535")


def test_negative_sla_duration_clamped_to_zero():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=a_datetime(year=2021, month=1, day=5),
        final_acknowledgement_time=a_datetime(year=2021, month=1, day=4),
    )

    expected_sla_duration = timedelta(0)
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    assert actual.sla_duration == expected_sla_duration


def test_produces_no_sla_given_no_request_completed_time():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=None,
        final_acknowledgement_time=None,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = None

    assert actual.sla_duration == expected_sla_duration


def test_produces_no_sla_given_no_final_acknowledgement_time():
    conversation = build_mock_gp2gp_conversation(
        request_completed_time=a_datetime(year=2021, month=1, day=5),
        final_acknowledgement_time=None,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = None

    assert actual.sla_duration == expected_sla_duration


def test_produces_no_sla_given_acks_with_only_duplicate_error():
    conversation = Gp2gpConversation(
        messages=test_cases.acknowledged_duplicate_and_waiting_for_integration(),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = None
    expected_date_completed = None

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == expected_date_completed


def test_produces_sla_given_integration_with_conflicting_acks_and_duplicate_ehrs():
    successful_acknowledgement_datetime = a_datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )

    conversation = Gp2gpConversation(
        messages=test_cases.ehr_integrated_with_conflicting_acks_and_duplicate_ehrs(
            request_completed_time=a_datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_given_suppression_with_conflicting_acks_and_duplicate_ehrs():
    successful_acknowledgement_datetime = a_datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversation = Gp2gpConversation(
        messages=test_cases.ehr_suppressed_with_conflicting_acks_and_duplicate_ehrs(
            request_completed_time=a_datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_given_failure_with_conflicting_acks_and_duplicate_ehrs():
    failed_acknowledgement_datetime = a_datetime(
        year=2020, month=6, day=1, hour=13, minute=52, second=0
    )
    conversation = Gp2gpConversation(
        messages=test_cases.integration_failed_with_conflicting_acks_and_duplicate_ehrs(
            request_completed_time=a_datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=failed_acknowledgement_datetime,
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = timedelta(hours=1, minutes=10)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == failed_acknowledgement_datetime


def test_produces_sla_given_integration_with_conflicting_duplicate_and_error_acks():
    successful_acknowledgement_datetime = a_datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )

    conversation = Gp2gpConversation(
        messages=test_cases.ehr_integrated_with_conflicting_duplicate_and_conflicting_error_ack(
            request_completed_time=a_datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = timedelta(hours=4, minutes=0, seconds=1)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_sla_given_suppression_with_conflicting_duplicate_and_error_acks():
    successful_acknowledgement_datetime = a_datetime(
        year=2020, month=6, day=1, hour=16, minute=42, second=1
    )

    conversation = Gp2gpConversation(
        messages=test_cases.ehr_suppressed_with_conflicting_duplicate_and_conflicting_error_ack(
            request_completed_time=a_datetime(
                year=2020, month=6, day=1, hour=12, minute=42, second=0
            ),
            ehr_acknowledge_time=successful_acknowledgement_datetime,
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_sla_duration = timedelta(hours=4, minutes=0, seconds=1)

    assert actual.sla_duration == expected_sla_duration
    assert actual.date_completed == successful_acknowledgement_datetime


def test_produces_last_sender_message_timestamp_given_an_integrated_ontime_transfer():
    request_completed_date = a_datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0)

    conversation = Gp2gpConversation(
        messages=test_cases.ehr_integrated_successfully(
            request_completed_time=request_completed_date
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_last_sender_message_timestamp = request_completed_date

    assert actual.last_sender_message_timestamp == expected_last_sender_message_timestamp


def test_produces_last_sender_message_timestamp_given_an_integrated_ontime_transfer_with_copcs():
    request_completed_date = a_datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0)

    conversation = Gp2gpConversation(
        messages=test_cases.successful_integration_with_copc_fragments(
            request_completed_time=request_completed_date
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_last_sender_message_timestamp = request_completed_date

    assert actual.last_sender_message_timestamp == expected_last_sender_message_timestamp


def test_produces_last_sender_message_timestamp_given_core_ehr_sent():
    request_completed_date = a_datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0)

    conversation = Gp2gpConversation(
        messages=test_cases.core_ehr_sent(request_completed_time=request_completed_date),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_last_sender_message_timestamp = request_completed_date

    assert actual.last_sender_message_timestamp == expected_last_sender_message_timestamp


def test_produces_last_sender_message_timestamp_given_copc_fragment_failure():
    copc_fragment_time = a_datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0)

    conversation = Gp2gpConversation(
        messages=test_cases.copc_fragment_failure(copc_fragment_time=copc_fragment_time),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_last_sender_message_timestamp = copc_fragment_time

    assert actual.last_sender_message_timestamp == expected_last_sender_message_timestamp


def test_produces_none_as_last_sender_message_timestamp_given_request_made():
    conversation = Gp2gpConversation(
        messages=test_cases.request_made(),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    assert actual.last_sender_message_timestamp is None


def test_produces_last_sender_message_timestamp_given_request_acked_successfully():
    request_acknowledged_date = a_datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0)

    conversation = Gp2gpConversation(
        messages=test_cases.request_acknowledged_successfully(
            sender_ack_time=request_acknowledged_date
        ),
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)
    expected = request_acknowledged_date
    assert actual.last_sender_message_timestamp == expected


def test_produces_last_sender_message_timestamp_from_request_completed_before_integration_only():
    request_completed_date = a_datetime(year=2020, month=6, day=1, hour=12, minute=42, second=0)

    messages = test_cases.ehr_integrated_with_duplicate_having_second_sender_ack_after_integration(
        request_completed_time=request_completed_date
    )

    conversation = Gp2gpConversation(
        messages=messages,
        probe=mock_gp2gp_conversation_observability_probe,
    )
    mock_lookup = Mock()

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)

    expected_last_sender_message_timestamp = request_completed_date

    assert actual.last_sender_message_timestamp == expected_last_sender_message_timestamp


def test_produces_sending_practice_ods_code_using_organisation_lookup():
    conversation = build_mock_gp2gp_conversation(sending_practice_asid="100")
    mock_lookup = Mock()
    mock_lookup.practice_ods_code_from_asid.return_value = "AB123"

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)
    expected_sending_practice_ods_code = "AB123"

    assert actual.sending_practice.ods_code == expected_sending_practice_ods_code
    mock_lookup.practice_ods_code_from_asid.assert_any_call("100")


def test_produces_requesting_practice_ods_code_using_organisation_lookup():
    conversation = build_mock_gp2gp_conversation(requesting_practice_asid="101")
    mock_lookup = Mock()
    mock_lookup.practice_ods_code_from_asid.return_value = "AB123"

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)
    expected_requesting_practice_ods_code = "AB123"

    assert actual.requesting_practice.ods_code == expected_requesting_practice_ods_code
    mock_lookup.practice_ods_code_from_asid.assert_any_call("101")


def test_produces_no_practice_ods_code_using_organisation_lookup_when_no_ods_mapping_exists():
    conversation = build_mock_gp2gp_conversation()
    mock_lookup = Mock()
    mock_lookup.practice_ods_code_from_asid.return_value = None

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)
    expected_requesting_practice_ods_code = None
    expected_sending_practice_ods_code = None

    assert actual.requesting_practice.ods_code == expected_requesting_practice_ods_code
    assert actual.sending_practice.ods_code == expected_sending_practice_ods_code


def test_produces_sending_practice_sicbl_ods_code_using_organisation_lookup():
    conversation = build_mock_gp2gp_conversation(sending_practice_asid="100")
    mock_lookup = Mock()
    mock_lookup.practice_ods_code_from_asid.return_value = "AB123"
    mock_lookup.sicbl_ods_code_from_practice_ods_code.return_value = "10A"

    transfer_service = TransferService(
        cutoff=timedelta(days=14),
        observability_probe=mock_transfer_observability_probe,
    )

    actual = transfer_service.derive_transfer(conversation, mock_lookup)
    expected_sending_practice_sicbl_ods_code = "10A"

    assert actual.sending_practice.sicbl_ods_code == expected_sending_practice_sicbl_ods_code
    mock_lookup.practice_ods_code_from_asid.assert_any_call("100")
    mock_lookup.sicbl_ods_code_from_practice_ods_code.assert_any_call("AB123")
