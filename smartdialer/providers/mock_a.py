import asyncio
import random
import uuid
from datetime import datetime, timezone
from smartdialer.providers.base import ProviderEvent

class MockProviderA:
    """Fast, reliable, in-order, no duplicates. `answer_rate` and `avg_talk_time` are real
    behavioral inputs (fix #8), not labels: they drive whether/when a simulated call reaches
    ANSWERED and how long it stays CONNECTED before COMPLETED."""

    def __init__(self, seed: int | None = None, answer_rate: float = 0.95, avg_talk_time: float = 120):
        self._rng = random.Random(seed)
        self._answer_rate = answer_rate
        self._avg_talk_time = avg_talk_time
        self._events: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._idempotency_index: dict[str, str] = {}
        self._call_status: dict[str, str] = {}
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
        await self._events.put(self._make_event(provider_call_id, "INITIATED"))
        self._spawn(self._simulate_call(provider_call_id))
        return provider_call_id

    def _make_event(self, provider_call_id: str, event_type: str) -> ProviderEvent:
        return ProviderEvent(
            provider_event_id=str(uuid.uuid4()),
            provider_call_id=provider_call_id,
            event_type=event_type,
            event_timestamp=datetime.now(timezone.utc),
        )

    async def _advance(self, provider_call_id: str, event_type: str, delay: float):
        # State and delivery move together here (in-order, no shuffling) but remain two
        # separate statements — Provider B below is where they intentionally diverge.
        await asyncio.sleep(delay)
        self._call_status[provider_call_id] = event_type
        await self._events.put(self._make_event(provider_call_id, event_type))

    async def _simulate_call(self, provider_call_id: str):
        await self._advance(provider_call_id, "RINGING", self._rng.uniform(0.01, 0.05))
        if self._rng.random() < self._answer_rate:
            await self._advance(provider_call_id, "ANSWERED", self._rng.uniform(0.01, 0.05))
            # avg_talk_time is scaled down for test/simulation wall-clock speed; relative
            # ordering across scenarios (e.g. scenario C's 180s vs scenario B's 90s) is what
            # the Predictive Pacing Engine and simulation harness care about, not real seconds.
            talk_time = self._rng.uniform(self._avg_talk_time * 0.05, self._avg_talk_time * 0.15)
            await self._advance(provider_call_id, "COMPLETED", talk_time)
        else:
            await self._advance(provider_call_id, "FAILED", self._rng.uniform(0.01, 0.05))

    async def next_event(self) -> ProviderEvent:
        return await self._events.get()

    async def get_call_status(self, provider_call_id: str) -> str | None:
        return self._call_status.get(provider_call_id)
