from dataclasses import dataclass
from sqlalchemy import text
from smartdialer.reservation import claim_available_agents, claim_available_borrowers

@dataclass
class DialPlan:
    agent_bound_count: int
    predictive_unassigned_count: int
    reasoning: str

class CallAllocator:
    async def execute(self, sql_engine, plan: DialPlan, campaign_id: int, worker_id: str, provider) -> list[str]:
        call_ids: list[str] = []
        call_ids += await self._allocate_agent_bound(sql_engine, plan.agent_bound_count, campaign_id, worker_id, provider)
        call_ids += await self._allocate_predictive_unassigned(
            sql_engine, plan.predictive_unassigned_count, campaign_id, worker_id, provider
        )
        return call_ids

    async def _allocate_agent_bound(self, sql_engine, n, campaign_id, worker_id, provider) -> list[str]:
        if n <= 0:
            return []

        # Transaction 1: reserve resources and create RESERVED call rows. Committed and
        # closed before any provider call is made.
        with sql_engine.begin() as conn:
            agent_ids = claim_available_agents(conn, n, worker_id)
            borrower_ids = claim_available_borrowers(conn, campaign_id, len(agent_ids), worker_id)
            pairs = list(zip(agent_ids, borrower_ids))
            unused_agents = agent_ids[len(pairs):]
            for agent_id in unused_agents:
                conn.execute(text(
                    "UPDATE agents SET status='AVAILABLE', worker_id=NULL WHERE id=:id"
                ), {"id": agent_id})
            created = [
                (self._create_call(conn, campaign_id, borrower_id, agent_id, "AGENT_BOUND", worker_id), agent_id)
                for agent_id, borrower_id in pairs
            ]

        # No transaction open here: call the provider, then persist the result separately.
        call_ids = []
        for call_id, _agent_id in created:
            provider_call_id = await provider.place_call(call_id, "sim-phone", idempotency_key=call_id)
            with sql_engine.begin() as conn:
                conn.execute(text(
                    "UPDATE calls SET status='INITIATED', provider_call_id=:pcid, updated_at=now() "
                    "WHERE id=:id AND status='RESERVED'"
                ), {"pcid": provider_call_id, "id": call_id})
            call_ids.append(call_id)
        return call_ids

    async def _allocate_predictive_unassigned(self, sql_engine, n, campaign_id, worker_id, provider) -> list[str]:
        if n <= 0:
            return []

        with sql_engine.begin() as conn:
            borrower_ids = claim_available_borrowers(conn, campaign_id, n, worker_id)
            created = [
                self._create_call(conn, campaign_id, borrower_id, None, "PREDICTIVE_UNASSIGNED", worker_id)
                for borrower_id in borrower_ids
            ]

        call_ids = []
        for call_id in created:
            provider_call_id = await provider.place_call(call_id, "sim-phone", idempotency_key=call_id)
            with sql_engine.begin() as conn:
                conn.execute(text(
                    "UPDATE calls SET status='INITIATED', provider_call_id=:pcid, updated_at=now() "
                    "WHERE id=:id AND status='RESERVED'"
                ), {"pcid": provider_call_id, "id": call_id})
            call_ids.append(call_id)
        return call_ids

    def _create_call(self, conn, campaign_id, borrower_id, agent_id, allocation_mode, worker_id) -> str:
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, "
            "worker_id, reserved_at, lease_expires_at) "
            "VALUES (:cid, :bid, :aid, 'RESERVED', :mode, :wid, now(), now() + interval '30 seconds') "
            "RETURNING id"
        ), {"cid": campaign_id, "bid": borrower_id, "aid": agent_id, "mode": allocation_mode, "wid": worker_id}
        ).fetchone()
        return str(row[0])
