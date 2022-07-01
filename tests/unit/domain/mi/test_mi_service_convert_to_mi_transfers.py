from prmdata.domain.mi.event_type import EventType
from prmdata.domain.mi.mi_message import MiMessagePayloadRegistration
from prmdata.domain.mi.mi_service import MiService
from prmdata.domain.mi.mi_transfer import EventSummary, MiPractice, MiTransfer
from tests.builders.common import a_datetime, a_string
from tests.builders.mi_message import (
    build_mi_message,
    build_mi_message_payload,
    build_mi_message_payload_registration,
)


def test_convert_to_mi_transfers():
    conversation_id_one = a_string()
    event_id_one_a = a_string()
    event_type_one_a = EventType.EHR_REQUESTED
    event_generated_datetime_one_a = a_datetime()
    requesting_practice_supplier_one_a = a_string()
    requesting_practice_ods_code_one_a = a_string()

    event_id_one_b = a_string()
    event_type_one_b = EventType.REGISTRATION_STARTED
    event_generated_datetime_one_b = a_datetime()
    sending_practice_supplier_one_b = a_string()
    sending_practice_ods_code_one_b = a_string()

    conversation_id_two = a_string()
    event_id_two = a_string()
    event_type_two = EventType.EHR_GENERATED
    event_generated_datetime_two = a_datetime()
    requesting_practice_supplier_two = a_string()
    requesting_practice_ods_code_two = a_string()
    sending_practice_ods_code_two = a_string()

    grouped_messages = {
        conversation_id_one: [
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_a,
                event_type=event_type_one_a,
                event_generated_datetime=event_generated_datetime_one_a,
                reporting_system_supplier=requesting_practice_supplier_one_a,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_one_a,
                        sending_practice_ods_code=sending_practice_ods_code_one_b,
                    )
                ),
            ),
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_b,
                event_type=event_type_one_b,
                event_generated_datetime=event_generated_datetime_one_b,
                reporting_system_supplier=sending_practice_supplier_one_b,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code="non_used_ods_code",
                        sending_practice_ods_code="non_used_ods_code",
                    )
                ),
            ),
        ],
        conversation_id_two: [
            build_mi_message(
                conversation_id=conversation_id_two,
                event_id=event_id_two,
                event_type=event_type_two,
                event_generated_datetime=event_generated_datetime_two,
                reporting_system_supplier=requesting_practice_supplier_two,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_two,
                        sending_practice_ods_code=sending_practice_ods_code_two,
                    )
                ),
            )
        ],
    }

    expected = [
        MiTransfer(
            conversation_id=conversation_id_one,
            events=[
                EventSummary(
                    event_generated_datetime=event_generated_datetime_one_a.isoformat(),
                    event_type=event_type_one_a,
                    event_id=event_id_one_a,
                ),
                EventSummary(
                    event_generated_datetime=event_generated_datetime_one_b.isoformat(),
                    event_type=event_type_one_b,
                    event_id=event_id_one_b,
                ),
            ],
            sending_practice=MiPractice(
                supplier=sending_practice_supplier_one_b, ods_code=sending_practice_ods_code_one_b
            ),
            requesting_practice=MiPractice(
                supplier=requesting_practice_supplier_one_a,
                ods_code=requesting_practice_ods_code_one_a,
            ),
            slow_transfer=None,
        ),
        MiTransfer(
            conversation_id=conversation_id_two,
            events=[
                EventSummary(
                    event_generated_datetime=event_generated_datetime_two.isoformat(),
                    event_type=event_type_two,
                    event_id=event_id_two,
                )
            ],
            sending_practice=MiPractice(supplier=None, ods_code=sending_practice_ods_code_two),
            requesting_practice=MiPractice(
                supplier=requesting_practice_supplier_two, ods_code=requesting_practice_ods_code_two
            ),
            slow_transfer=None,
        ),
    ]

    actual = MiService().convert_to_mi_transfers(grouped_messages)

    assert actual == expected


def test_convert_to_mi_transfers_continues_if_missing_requesting_practice_ods_code():
    conversation_id_one = a_string()
    event_id_one_a = a_string()
    event_type_one_a = EventType.EHR_REQUESTED
    event_generated_datetime_one_a = a_datetime()
    requesting_practice_supplier_one_a = a_string()

    event_id_one_b = a_string()
    event_type_one_b = EventType.REGISTRATION_STARTED
    event_generated_datetime_one_b = a_datetime()
    sending_practice_supplier_one_b = a_string()

    grouped_messages = {
        conversation_id_one: [
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_a,
                event_type=event_type_one_a,
                event_generated_datetime=event_generated_datetime_one_a,
                reporting_system_supplier=requesting_practice_supplier_one_a,
                payload=build_mi_message_payload(
                    registration=MiMessagePayloadRegistration(
                        registration_type=None,
                        requesting_practice_ods_code=None,
                        sending_practice_ods_code=None,
                    )
                ),
            ),
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_b,
                event_type=event_type_one_b,
                event_generated_datetime=event_generated_datetime_one_b,
                reporting_system_supplier=sending_practice_supplier_one_b,
                payload=build_mi_message_payload(
                    registration=MiMessagePayloadRegistration(
                        registration_type=None,
                        requesting_practice_ods_code=None,
                        sending_practice_ods_code=None,
                    )
                ),
            ),
        ],
    }

    expected = [
        MiTransfer(
            conversation_id=conversation_id_one,
            events=[
                EventSummary(
                    event_generated_datetime=event_generated_datetime_one_a.isoformat(),
                    event_type=event_type_one_a,
                    event_id=event_id_one_a,
                ),
                EventSummary(
                    event_generated_datetime=event_generated_datetime_one_b.isoformat(),
                    event_type=event_type_one_b,
                    event_id=event_id_one_b,
                ),
            ],
            sending_practice=MiPractice(supplier=sending_practice_supplier_one_b, ods_code=None),
            requesting_practice=MiPractice(
                supplier=requesting_practice_supplier_one_a,
                ods_code=None,
            ),
            slow_transfer=None,
        ),
    ]

    actual = MiService().convert_to_mi_transfers(grouped_messages)

    assert actual == expected


def test_slow_transfer_returns_true_given_more_than_a_day_after_requested():
    conversation_id_one = a_string()
    event_id_one_a = a_string()
    event_type_one_a = EventType.EHR_REQUESTED
    transfer_event_datetime_one_a = a_datetime(year=2000, month=1, day=1)
    requesting_practice_supplier_one_a = a_string()
    requesting_practice_ods_code_one_a = a_string()

    event_id_one_b = a_string()
    event_type_one_b = EventType.EHR_VALIDATED
    transfer_event_datetime_one_b = a_datetime(year=2000, month=1, day=3)
    sending_practice_supplier_one_b = a_string()
    sending_practice_ods_code_one_b = a_string()

    conversation_id_two = a_string()
    event_id_two_a = a_string()
    event_type_two_a = EventType.MIGRATE_STRUCTURED_RECORD_REQUEST
    transfer_event_datetime_two_a = a_datetime(year=2000, month=1, day=1)
    requesting_practice_supplier_two_a = a_string()
    requesting_practice_ods_code_two_a = a_string()

    event_id_two_b = a_string()
    event_type_two_b = EventType.EHR_READY_TO_INTEGRATE
    transfer_event_datetime_two_b = a_datetime(year=2000, month=1, day=3)
    sending_practice_supplier_two_b = a_string()
    sending_practice_ods_code_two_b = a_string()

    grouped_messages = {
        conversation_id_one: [
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_a,
                event_type=event_type_one_a,
                event_generated_datetime=transfer_event_datetime_one_a,
                transfer_event_datetime=transfer_event_datetime_one_a,
                reporting_system_supplier=requesting_practice_supplier_one_a,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_one_a,
                        sending_practice_ods_code=sending_practice_ods_code_one_b,
                    )
                ),
            ),
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_b,
                event_type=event_type_one_b,
                event_generated_datetime=transfer_event_datetime_one_b,
                transfer_event_datetime=transfer_event_datetime_one_b,
                reporting_system_supplier=sending_practice_supplier_one_b,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_one_a,
                        sending_practice_ods_code=sending_practice_ods_code_one_b,
                    )
                ),
            ),
        ],
        conversation_id_two: [
            build_mi_message(
                conversation_id=conversation_id_two,
                event_id=event_id_two_a,
                event_type=event_type_two_a,
                event_generated_datetime=transfer_event_datetime_two_a,
                transfer_event_datetime=transfer_event_datetime_two_a,
                reporting_system_supplier=requesting_practice_supplier_two_a,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_two_a,
                        sending_practice_ods_code=sending_practice_ods_code_two_b,
                    )
                ),
            ),
            build_mi_message(
                conversation_id=conversation_id_two,
                event_id=event_id_two_b,
                event_type=event_type_two_b,
                event_generated_datetime=transfer_event_datetime_two_b,
                transfer_event_datetime=transfer_event_datetime_two_b,
                reporting_system_supplier=sending_practice_supplier_two_b,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_two_a,
                        sending_practice_ods_code=sending_practice_ods_code_two_b,
                    )
                ),
            ),
        ],
    }

    expected = [
        MiTransfer(
            conversation_id=conversation_id_one,
            events=[
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_one_a.isoformat(),
                    event_type=event_type_one_a,
                    event_id=event_id_one_a,
                ),
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_one_b.isoformat(),
                    event_type=event_type_one_b,
                    event_id=event_id_one_b,
                ),
            ],
            sending_practice=MiPractice(
                supplier=sending_practice_supplier_one_b, ods_code=sending_practice_ods_code_one_b
            ),
            requesting_practice=MiPractice(
                supplier=requesting_practice_supplier_one_a,
                ods_code=requesting_practice_ods_code_one_a,
            ),
            slow_transfer=True,
        ),
        MiTransfer(
            conversation_id=conversation_id_two,
            events=[
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_two_a.isoformat(),
                    event_type=event_type_two_a,
                    event_id=event_id_two_a,
                ),
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_two_b.isoformat(),
                    event_type=event_type_two_b,
                    event_id=event_id_two_b,
                ),
            ],
            sending_practice=MiPractice(
                supplier=sending_practice_supplier_two_b, ods_code=sending_practice_ods_code_two_b
            ),
            requesting_practice=MiPractice(
                supplier=requesting_practice_supplier_two_a,
                ods_code=requesting_practice_ods_code_two_a,
            ),
            slow_transfer=True,
        ),
    ]

    actual = MiService().convert_to_mi_transfers(grouped_messages)

    assert actual == expected


def test_slow_transfer_returns_false_given_less_than_a_day_after_requested():
    conversation_id_one = a_string()
    event_id_one_a = a_string()
    event_type_one_a = EventType.EHR_REQUESTED
    transfer_event_datetime_one_a = a_datetime(year=2000, month=1, day=1, hour=1)
    requesting_practice_supplier_one_a = a_string()
    requesting_practice_ods_code_one_a = a_string()

    event_id_one_b = a_string()
    event_type_one_b = EventType.EHR_VALIDATED
    transfer_event_datetime_one_b = a_datetime(year=2000, month=1, day=1, hour=23)
    sending_practice_supplier_one_b = a_string()
    sending_practice_ods_code_one_b = a_string()

    conversation_id_two = a_string()
    event_id_two_a = a_string()
    event_type_two_a = EventType.EHR_READY_TO_INTEGRATE
    transfer_event_datetime_two_a = a_datetime(year=2000, month=1, day=1, hour=1)
    requesting_practice_supplier_two_a = a_string()
    requesting_practice_ods_code_two_a = a_string()

    event_id_two_b = a_string()
    event_type_two_b = EventType.MIGRATE_STRUCTURED_RECORD_REQUEST
    transfer_event_datetime_two_b = a_datetime(year=2000, month=1, day=1, hour=23)
    sending_practice_supplier_two_b = a_string()
    sending_practice_ods_code_two_b = a_string()

    grouped_messages = {
        conversation_id_one: [
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_a,
                event_type=event_type_one_a,
                event_generated_datetime=transfer_event_datetime_one_a,
                transfer_event_datetime=transfer_event_datetime_one_a,
                reporting_system_supplier=requesting_practice_supplier_one_a,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_one_a,
                        sending_practice_ods_code=sending_practice_ods_code_one_b,
                    )
                ),
            ),
            build_mi_message(
                conversation_id=conversation_id_one,
                event_id=event_id_one_b,
                event_type=event_type_one_b,
                event_generated_datetime=transfer_event_datetime_one_b,
                transfer_event_datetime=transfer_event_datetime_one_b,
                reporting_system_supplier=sending_practice_supplier_one_b,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_one_a,
                        sending_practice_ods_code=sending_practice_ods_code_one_b,
                    )
                ),
            ),
        ],
        conversation_id_two: [
            build_mi_message(
                conversation_id=conversation_id_two,
                event_id=event_id_two_a,
                event_type=event_type_two_a,
                event_generated_datetime=transfer_event_datetime_two_a,
                transfer_event_datetime=transfer_event_datetime_two_a,
                reporting_system_supplier=requesting_practice_supplier_two_a,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_two_a,
                        sending_practice_ods_code=sending_practice_ods_code_two_b,
                    )
                ),
            ),
            build_mi_message(
                conversation_id=conversation_id_two,
                event_id=event_id_two_b,
                event_type=event_type_two_b,
                event_generated_datetime=transfer_event_datetime_two_b,
                transfer_event_datetime=transfer_event_datetime_two_b,
                reporting_system_supplier=sending_practice_supplier_two_b,
                payload=build_mi_message_payload(
                    registration=build_mi_message_payload_registration(
                        requesting_practice_ods_code=requesting_practice_ods_code_two_a,
                        sending_practice_ods_code=sending_practice_ods_code_two_b,
                    )
                ),
            ),
        ],
    }

    expected = [
        MiTransfer(
            conversation_id=conversation_id_one,
            events=[
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_one_a.isoformat(),
                    event_type=event_type_one_a,
                    event_id=event_id_one_a,
                ),
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_one_b.isoformat(),
                    event_type=event_type_one_b,
                    event_id=event_id_one_b,
                ),
            ],
            sending_practice=MiPractice(
                supplier=sending_practice_supplier_one_b, ods_code=sending_practice_ods_code_one_b
            ),
            requesting_practice=MiPractice(
                supplier=requesting_practice_supplier_one_a,
                ods_code=requesting_practice_ods_code_one_a,
            ),
            slow_transfer=False,
        ),
        MiTransfer(
            conversation_id=conversation_id_two,
            events=[
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_two_a.isoformat(),
                    event_type=event_type_two_a,
                    event_id=event_id_two_a,
                ),
                EventSummary(
                    event_generated_datetime=transfer_event_datetime_two_b.isoformat(),
                    event_type=event_type_two_b,
                    event_id=event_id_two_b,
                ),
            ],
            sending_practice=MiPractice(
                supplier=sending_practice_supplier_two_b, ods_code=sending_practice_ods_code_two_b
            ),
            requesting_practice=MiPractice(
                supplier=requesting_practice_supplier_two_a,
                ods_code=requesting_practice_ods_code_two_a,
            ),
            slow_transfer=False,
        ),
    ]

    actual = MiService().convert_to_mi_transfers(grouped_messages)

    assert actual == expected
