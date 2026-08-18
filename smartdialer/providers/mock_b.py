import asyncio
import random
import uuid
from datetime import datetime, timezone
from smartdialer.providers.base import ProviderEvent

class MockProviderB:
    """Slower, occasional timeouts, duplicate events, out-of-order delivery.

    Fix #9: authoritative state (`_call_status`) always advances in TRUE chronological
    order, synchronously, inside `_simulate_call` (see `_advance_and_schedule_delivery`).
    Delivery is spun off as an independent fire-and-forget task with its own random
    delay (`_deliver_later` / `_deliver`) so it can duplicate and land out of order
    relative to other phases' deliveries — it never touches `_call_status`. The Lease
    Reaper (Task 12) relies on `get_call_status()` reflecting reality even when delivery
    is shuffled or delayed.
    """

    def __init__(self, seed: int | None = None, answer_rate: float = 0.5,
                 avg_talk_time: float = 120, force_duplicate: bool = False):
        self._rng = random.Random(seed)
        self._answer_rate = answer_rate
        self._avg_talk_time = avg_talk_time
        self._events: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._idempotency_index: dict[str, str] = {}
        self._call_status: dict[str, str] = {}
        self._force_duplicate = force_duplicate
        # ponytail: standard fire-and-forget idiom — asyncio only weak-refs tasks
        # internally, so an unreferenced task can be GC'd mid-run.
        self._background_tasks: set[asyncio.Task] = set()

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def place_call(self, call_id: str, phone_number: str, idempotency_key: str) -> str:
        if idempotency_key in self._idempotency_index:
            return self._idempotency_index[idempotency_key]
        provider_call_id = str(uuid.uuid4())
        self._idempotency_index[idempotency_key] = provider_call_id
        self._call_status[provider_call_id] = "INITIATED"
        await self._deliver(self._make_event(provider_call_id, "INITIATED"))
        self._spawn(self._simulate_call(provider_call_id))
        return provider_call_id

    def _make_event(self, provider_call_id: str, event_type: str) -> ProviderEvent:
        return ProviderEvent(
            provider_event_id=str(uuid.uuid4()),
            provider_call_id=provider_call_id,
            event_type=event_type,
            event_timestamp=datetime.now(timezone.utc),
        )

    async def _deliver(self, event: ProviderEvent):
        """Delivery-only: never touches _call_status. May duplicate."""
        await self._events.put(event)
        if self._force_duplicate or self._rng.random() < 0.1:
            await asyncio.sleep(self._rng.uniform(0.01, 0.05))
            await self._events.put(event)  # same provider_event_id: a genuine duplicate

    async def _deliver_later(self, event: ProviderEvent, delay: float):
        # Fire-and-forget: this task's own random delay (independent of the main
        # _simulate_call coroutine's timing) is what lets delivery land out of order
        # relative to when _call_status actually advanced.
        await asyncio.sleep(delay)
        await self._deliver(event)

    def _advance_and_schedule_delivery(self, provider_call_id: str, event_type: str):
        # Authoritative state moves here, synchronously, in true chronological order.
        self._call_status[provider_call_id] = event_type
        event = self._make_event(provider_call_id, event_type)
        # Delivery is spun off as an independent task so it can race with (and land
        # out of order relative to) other phases' deliveries — it never touches
        # _call_status, which has already reached its true value above.
        self._spawn(self._deliver_later(event, self._rng.uniform(0.0, 0.3)))

    async def _simulate_call(self, provider_call_id: str):
        if self._rng.random() < 0.05:
            return  # simulated timeout: authoritative state stays INITIATED, no more events

        await asyncio.sleep(self._rng.uniform(0.05, 0.3))
        self._advance_and_schedule_delivery(provider_call_id, "RINGING")

        if self._rng.random() < self._answer_rate:
            await asyncio.sleep(self._rng.uniform(0.05, 0.3))
            self._advance_and_schedule_delivery(provider_call_id, "ANSWERED")

            talk_time = self._rng.uniform(self._avg_talk_time * 0.05, self._avg_talk_time * 0.15)
            await asyncio.sleep(talk_time)
            self._advance_and_schedule_delivery(provider_call_id, "COMPLETED")
        else:
            await asyncio.sleep(self._rng.uniform(0.05, 0.3))
            self._advance_and_schedule_delivery(provider_call_id, "FAILED")

    async def next_event(self) -> ProviderEvent:
        return await self._events.get()

    async def get_call_status(self, provider_call_id: str) -> str | None:
        return self._call_status.get(provider_call_id)
