from sqlalchemy import text


class ProgressivePacingEngine:
    def recommend(self, conn, campaign_id: int) -> tuple[int, str]:
        available = conn.execute(text(
            "SELECT count(*) FROM agents WHERE status='AVAILABLE'"
        )).scalar()
        return available, f"{available} agents currently available; request one call per available agent"
