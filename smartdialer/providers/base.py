from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

@dataclass
class ProviderEvent:
    provider_event_id: str
    provider_call_id: str
    event_type: str
    event_timestamp: datetime

class Provider(Protocol):
    async def place_call(self, call_id: str, phone_number: str, idempotency_key: str) -> str: ...
    async def next_event(self) -> ProviderEvent: ...
    async def get_call_status(self, provider_call_id: str) -> str | None: ...
