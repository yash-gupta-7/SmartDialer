from typing import Protocol


class PacingEngine(Protocol):
    def recommend(self, conn, campaign_id: int) -> tuple[int, str]: ...
