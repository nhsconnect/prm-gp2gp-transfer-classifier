from dataclasses import dataclass
from typing import List, Optional


@dataclass
class MiMessagePayloadRegistration:
    registrationStartedDateTime: Optional[str]
    registrationType: Optional[str]
    requestingPracticeOdsCode: Optional[str]
    sendingPracticeOdsCode: Optional[str]


@dataclass
class MiMessagePayloadIntegration:
    integrationStatus: str
    reason: str


@dataclass
class Coding:
    code: str
    system: str


@dataclass
class Degrade:
    type: str
    metadata: str
    code: List[Coding]


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
class MiMessagePayloadEhr:
    ehr_total_size_bytes: Optional[int]
    ehr_structured_size_bytes: Optional[int]
    degrade: Optional[List[Degrade]]
    attachment: Optional[List[Attachment]]
    placeholder: Optional[List[Placeholder]]


@dataclass
class MiMessagePayload:
    registration: Optional[MiMessagePayloadRegistration]
    integration: Optional[MiMessagePayloadIntegration]
    ehr: Optional[MiMessagePayloadEhr]


@dataclass
class MiMessage:
    conversation_id: str
    event_id: str
    event_type: str
    transfer_protocol: str
    event_generated_datetime: str
    reporting_system_supplier: str
    reporting_practice_ods_code: str
    transfer_event_datetime: str
    payload: MiMessagePayload
