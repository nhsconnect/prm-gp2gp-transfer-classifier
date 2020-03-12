from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class PracticeDetails:
    ods_code: str
    name: str


@dataclass
class PracticeMetadata:
    generated_on: datetime
    practices: List[PracticeDetails]
