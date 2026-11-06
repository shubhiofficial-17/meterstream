"""Storage for readings and incidents."""

from __future__ import annotations

from typing import Protocol

from meterstream.models import Incident, Reading


class Store(Protocol):
    def write_readings(self, readings: list[Reading]) -> int: ...  # returns rows actually inserted

    def upsert_incidents(self, incidents: list[Incident]) -> None: ...

    def readings(self, meter_id: str, start: int, end: int, bucket_s: int = 900) -> list[dict]: ...

    def incidents(
        self, meter_id: str | None = None, kind: str | None = None, open_only: bool = False, limit: int = 100
    ) -> list[dict]: ...

    def fleet_summary(self) -> dict: ...
