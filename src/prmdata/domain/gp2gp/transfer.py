from dataclasses import dataclass
from datetime import timedelta, datetime
from typing import NamedTuple, Optional, List
from enum import Enum


class TransferStatus(Enum):
    INTEGRATED_ON_TIME = "INTEGRATED_ON_TIME"
    TECHNICAL_FAILURE = "TECHNICAL_FAILURE"
    PROCESS_FAILURE = "PROCESS_FAILURE"
    UNCLASSIFIED_FAILURE = "UNCLASSIFIED_FAILURE"


class TransferFailureReason(Enum):
    INTEGRATED_LATE = "Integrated Late"
    FINAL_ERROR = "Final Error"
    TRANSFERRED_NOT_INTEGRATED = "Transferred, not integrated"
    REQUEST_NOT_ACKNOWLEDGED = "Request not Acknowledged"
    CORE_EHR_NOT_SENT = "Core Extract not Sent"
    FATAL_SENDER_ERROR = "Contains Fatal Sender Error"
    COPC_NOT_SENT = "COPC(s) not sent"
    COPC_NOT_ACKNOWLEDGED = "COPC(s) not Acknowledged"
    TRANSFERRED_NOT_INTEGRATED_WITH_ERROR = "Transferred, not integrated, with error"
    AMBIGUOUS_COPCS = "Ambiguous COPC messages"


@dataclass
class TransferOutcome:
    status: TransferStatus
    failure_reason: Optional[TransferFailureReason]


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
