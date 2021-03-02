from typing import Iterable

from prmdata.domain.gp2gp.practice_metrics import PracticeMetrics, IntegratedPracticeMetrics
from prmdata.domain.gp2gp.transfer import Transfer, TransferStatus
from tests.builders.common import a_string, a_duration, an_integer, a_datetime


def build_transfer(**kwargs):
    return Transfer(
        conversation_id=kwargs.get("conversation_id", a_string(36)),
        sla_duration=kwargs.get("sla_duration", a_duration()),
        requesting_practice_asid=kwargs.get("requesting_practice_asid", a_string(12)),
        sending_practice_asid=kwargs.get("sending_practice_asid", a_string(12)),
        final_error_code=kwargs.get("final_error_code", None),
        intermediate_error_codes=kwargs.get("intermediate_error_codes", []),
        status=kwargs.get("status", TransferStatus.PENDING),
        date_requested=kwargs.get("date_requested", a_datetime()),
        date_completed=kwargs.get("date_completed", None),
    )


def build_practice_metrics(**kwargs):
    return PracticeMetrics(
        ods_code=kwargs.get("ods_code", a_string(6)),
        name=kwargs.get("name", a_string()),
        integrated=IntegratedPracticeMetrics(
            transfer_count=kwargs.get("transfer_count", an_integer()),
            within_3_days=kwargs.get("within_3_days", an_integer()),
            within_8_days=kwargs.get("within_8_days", an_integer()),
            beyond_8_days=kwargs.get("beyond_8_days", an_integer()),
        ),
    )


def build_transfers(**kwargs) -> Iterable[Transfer]:
    transfer_count = kwargs.get("transfer_count", an_integer(2, 7))
    integrated_transfer_count = kwargs.get("integrated_transfer_count", 0)
    sla_duration = kwargs.get("sla_duration", a_duration())
    transfers = []
    for _ in range(transfer_count):
        transfers.append(build_transfer())
    for _ in range(integrated_transfer_count):
        transfers.append(
            build_transfer(status=TransferStatus.INTEGRATED, sla_duration=sla_duration)
        )
    return transfers