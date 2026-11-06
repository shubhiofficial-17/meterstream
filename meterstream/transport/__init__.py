"""Message transport between meters (or the simulator) and the ingest workers."""

from __future__ import annotations

from typing import Protocol

STREAM = "meter-readings"
GROUP = "ingest"


class Broker(Protocol):
    def publish(self, messages: list[dict]) -> None: ...

    def consume(self, consumer: str, count: int = 500, block_ms: int = 1000) -> list[tuple[str, dict]]: ...

    def ack(self, ids: list[str]) -> None: ...

    def lag(self) -> int: ...  # messages published but not yet acknowledged

    def save_solar_registry(self, meter_ids: set[str]) -> None: ...

    def solar_registry(self) -> set[str]: ...
