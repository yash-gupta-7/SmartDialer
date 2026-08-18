import math
from sqlalchemy import text

SETUP_TIME_WINDOW_SECONDS = 30


class PredictivePacingEngine:
    def __init__(self, sample_size: int = 50, min_rate: float = 0.05, max_rate: float = 0.95,
                 setup_time_window_seconds: int = SETUP_TIME_WINDOW_SECONDS):
        self.sample_size = sample_size
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.setup_time_window_seconds = setup_time_window_seconds

    def recommend(self, conn, campaign_id: int) -> tuple[int, str]:
        available = conn.execute(text(
            "SELECT count(*) FROM agents WHERE status='AVAILABLE'"
        )).scalar()
        freeing_soon = conn.execute(text(
            "SELECT count(*) FROM agents WHERE estimated_free_at IS NOT NULL "
            "AND estimated_free_at <= now() + make_interval(secs => :window)"
        ), {"window": self.setup_time_window_seconds}).scalar()
        in_flight = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid AND status IN ('INITIATED','RINGING')"
        ), {"cid": campaign_id}).scalar()

        recent = conn.execute(text(
            "SELECT status FROM calls WHERE campaign_id=:cid AND status IN ('COMPLETED','FAILED','ABANDONED') "
            "ORDER BY updated_at DESC LIMIT :n"
        ), {"cid": campaign_id, "n": self.sample_size}).fetchall()

        if not recent:
            answer_rate = 0.3  # no history yet: conservative default
        else:
            answered = sum(1 for r in recent if r[0] in ("COMPLETED",))
            answer_rate = answered / len(recent)
        answer_rate = min(max(answer_rate, self.min_rate), self.max_rate)

        requested = math.ceil((available + freeing_soon) / answer_rate) - in_flight
        requested = max(requested, 0)

        reasoning = (
            f"{requested} requested: available={available} + freeing_soon={freeing_soon} "
            f"(estimated_free_at within {self.setup_time_window_seconds}s), "
            f"answer_rate={answer_rate:.2f} (rolling avg over last {min(len(recent), self.sample_size)} outcomes), "
            f"in_flight={in_flight}"
        )
        return requested, reasoning
