from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from prmdata.domain.mi.event_type import EventType


@dataclass
class MiMessagePayloadRegistration:
    registration_type: Optional[str]
    requesting_practice_ods_code: Optional[str]
    sending_practice_ods_code: Optional[str]


@dataclass
class MiMessagePayloadIntegration:
    integration_status: str
    reason: str


@dataclass
class Coding:
    code: str
    system: str


@dataclass
class Codings:
    coding: List[Coding]


@dataclass
class Degrade:
    type: str
    metadata: str
    code: Codings


@dataclass
class Attachment:
    attachment_id: str
    clinical_type: str
    mime_type: str
    size_bytes: str


@dataclass
class Placeholder:
    placeholder_id: str
    attachment_id: str
    generated_by: str
    reason: int
    original_mime_type: str


@dataclass
class Error:
    error_code: int
    error_description: str


@dataclass
class UnsupportedDataItem:
    type: str
    unique_identifier: str
    reason: str


@dataclass
class TransferCompatibilityStatus:
    status: str
    reason: str


@dataclass
class DemographicTraceStatus:
    status: str
    reason: str


@dataclass
class StructuredRecordMigration:
    status: str
    reason: str


@dataclass
class DocumentMigration:
    status: str
    reason: str


@dataclass
class MiMessagePayloadEhr:
    ehr_total_size_bytes: Optional[int]
    ehr_structured_size_bytes: Optional[int]
    degrade: Optional[List[Degrade]]
    attachment: Optional[List[Attachment]]
    placeholder: Optional[List[Placeholder]]
    unsupported_data_item: Optional[List[UnsupportedDataItem]]
    error: Optional[List[Error]]


@dataclass
class MiMessagePayload:
    registration: Optional[MiMessagePayloadRegistration]
    integration: Optional[MiMessagePayloadIntegration]
    ehr: Optional[MiMessagePayloadEhr]
    transfer_compatibility_status: Optional[TransferCompatibilityStatus]
    demographic_trace_status: Optional[DemographicTraceStatus]
    smartcard_present: Optional[bool]
    structured_record_migration: Optional[StructuredRecordMigration]
    document_migration: Optional[DocumentMigration]


@dataclass
class MiMessage:
    conversation_id: str
    event_id: str
    event_type: EventType
    transfer_protocol: str
    event_generated_datetime: datetime
    reporting_system_supplier: str
    reporting_practice_ods_code: str
    transfer_event_datetime: datetime
    payload: MiMessagePayload
