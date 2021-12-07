from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, NamedTuple, Optional

from prmdata.domain.gp2gp.transfer_outcome import TransferOutcome


@dataclass
class Practice:
    asid: str
    supplier: str


class Transfer(NamedTuple):
    conversation_id: str
    sla_duration: Optional[timedelta]
    requesting_practice: Practice
    sending_practice: Practice
    sender_error_codes: List[Optional[int]]
    final_error_codes: List[Optional[int]]
    intermediate_error_codes: List[int]
    outcome: TransferOutcome
    date_requested: datetime
    date_completed: Optional[datetime]
    last_sender_message_timestamp: Optional[datetime]

    @property
    def sla_duration_seconds(self) -> Optional[int]:
        if self.sla_duration is not None:
            return round(self.sla_duration.total_seconds())
        else:
            return None

    @property
    def requesting_practice_asid(self) -> str:
        return self.requesting_practice.asid

    @property
    def requesting_supplier(self) -> str:
        return self.requesting_practice.supplier

    @property
    def sending_practice_asid(self) -> str:
        return self.sending_practice.asid

    @property
    def sending_supplier(self) -> str:
        return self.sending_practice.supplier

    @property
    def status_description(self) -> str:
        return self.outcome.status.value

    @property
    def failure_reason(self) -> Optional[str]:
        failure_reason = self.outcome.failure_reason
        return None if failure_reason is None else failure_reason.value
