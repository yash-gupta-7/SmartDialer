import asyncio
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB

def test_provider_a_idempotent_place_call():
    async def run():
        p = MockProviderA(seed=1)
        id1 = await p.place_call("call-1", "+15550000", idempotency_key="call-1")
        id2 = await p.place_call("call-1", "+15550000", idempotency_key="call-1")
        return id1, id2
    id1, id2 = asyncio.run(run())
    assert id1 == id2

def test_provider_b_idempotent_place_call():
    async def run():
        p = MockProviderB(seed=1)
        id1 = await p.place_call("call-2", "+15550001", idempotency_key="call-2")
        id2 = await p.place_call("call-2", "+15550001", idempotency_key="call-2")
        return id1, id2
    id1, id2 = asyncio.run(run())
    assert id1 == id2

def test_provider_a_emits_events_in_order():
    async def run():
        p = MockProviderA(seed=1, answer_rate=1.0)
        await p.place_call("call-3", "+15550002", idempotency_key="call-3")
        events = []
        for _ in range(2):
            events.append(await asyncio.wait_for(p.next_event(), timeout=2))
        return events
    events = asyncio.run(run())
    types = [e.event_type for e in events]
    assert types == ["INITIATED", "RINGING"]

def test_provider_b_can_emit_duplicate_events():
    async def run():
        p = MockProviderB(seed=2, force_duplicate=True)
        await p.place_call("call-4", "+15550003", idempotency_key="call-4")
        events = []
        for _ in range(4):
            events.append(await asyncio.wait_for(p.next_event(), timeout=2))
        return events
    events = asyncio.run(run())
    event_ids = [e.provider_event_id for e in events]
    assert len(event_ids) != len(set(event_ids)), "expected at least one duplicate provider_event_id"

def test_high_answer_rate_mostly_reaches_answered():
    async def run():
        p = MockProviderA(seed=42, answer_rate=1.0)
        outcomes = []
        for i in range(10):
            pcid = await p.place_call(f"call-{i}", "+1555", idempotency_key=f"call-{i}")
            await asyncio.sleep(0.3)
            outcomes.append(await p.get_call_status(pcid))
        return outcomes
    outcomes = asyncio.run(run())
    assert all(o in ("ANSWERED", "COMPLETED") for o in outcomes)

def test_zero_answer_rate_never_reaches_answered():
    async def run():
        p = MockProviderA(seed=42, answer_rate=0.0)
        outcomes = []
        for i in range(10):
            pcid = await p.place_call(f"call-{i}", "+1555", idempotency_key=f"call-{i}")
            await asyncio.sleep(0.3)
            outcomes.append(await p.get_call_status(pcid))
        return outcomes
    outcomes = asyncio.run(run())
    assert all(o != "ANSWERED" and o != "COMPLETED" for o in outcomes)

def test_get_call_status_reflects_true_state_even_when_event_delivery_is_shuffled():
    async def run():
        p = MockProviderB(seed=7, answer_rate=1.0, avg_talk_time=0.05)
        pcid = await p.place_call("call-9", "+1555", idempotency_key="call-9")
        # Drain whatever events have arrived so far without assuming order.
        await asyncio.sleep(0.5)
        status = await p.get_call_status(pcid)
        return status
    status = asyncio.run(run())
    assert status in ("RINGING", "ANSWERED", "COMPLETED")  # never "unknown"/stale-by-delivery-order
