from __future__ import annotations

from collections import defaultdict

from meterstream.models import Incident, Reading


class MemoryStore:
    def __init__(self) -> None:
        self._readings: dict[str, dict[int, Reading]] = defaultdict(dict)
        self._incidents: dict[str, Incident] = {}

    def write_readings(self, readings: list[Reading]) -> int:
        inserted = 0
        for r in readings:
            series = self._readings[r.meter_id]
            if r.ts not in series:  # same semantics as ON CONFLICT DO NOTHING
                series[r.ts] = r
                inserted += 1
        return inserted

    def upsert_incidents(self, incidents: list[Incident]) -> None:
        for inc in incidents:
            self._incidents[inc.id] = inc

    def readings(self, meter_id: str, start: int, end: int, bucket_s: int = 900) -> list[dict]:
        buckets: dict[int, list[Reading]] = defaultdict(list)
        for ts, r in self._readings.get(meter_id, {}).items():
            if start <= ts < end:
                buckets[ts - ts % bucket_s].append(r)
        out = []
        for b in sorted(buckets):
            rs = buckets[b]
            out.append(
                {
                    "bucket": b,
                    "avg_power_kw": round(sum(r.power_kw for r in rs) / len(rs), 3),
                    "max_power_kw": max(r.power_kw for r in rs),
                    "min_voltage_v": min(r.voltage_v for r in rs),
                    "max_voltage_v": max(r.voltage_v for r in rs),
                    "energy_kwh": round(max(r.energy_import_kwh for r in rs) - min(r.energy_import_kwh for r in rs), 3),
                    "samples": len(rs),
                }
            )
        return out

    def incidents(self, meter_id=None, kind=None, open_only=False, limit=100) -> list[dict]:
        rows = [
            i
            for i in self._incidents.values()
            if (meter_id is None or i.meter_id == meter_id)
            and (kind is None or i.kind.value == kind)
            and (not open_only or i.open)
        ]
        rows.sort(key=lambda i: i.start_ts, reverse=True)
        return [i.to_dict() for i in rows[:limit]]

    def fleet_summary(self) -> dict:
        by_kind: dict[str, int] = defaultdict(int)
        for i in self._incidents.values():
            by_kind[i.kind.value] += 1
        latest = [max(s) for s in self._readings.values() if s]
        return {
            "meters": len(self._readings),
            "readings": sum(len(s) for s in self._readings.values()),
            "incidents_by_kind": dict(by_kind),
            "open_incidents": sum(1 for i in self._incidents.values() if i.open),
            "latest_ts": max(latest) if latest else None,
        }
