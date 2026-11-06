"""Merge streams of alerts into incidents, so operators see one event instead of fifty."""

from __future__ import annotations

from meterstream.models import Alert, AlertKind, Incident


class IncidentTracker:
    def __init__(self, interval_s: int = 300, merge_gap_intervals: int = 2):
        self.merge_gap_s = interval_s * merge_gap_intervals
        self._open: dict[tuple[str, AlertKind], Incident] = {}

    def add(self, alert: Alert) -> tuple[Incident, bool]:
        """Returns (incident, is_new)."""
        key = (alert.meter_id, alert.kind)
        inc = self._open.get(key)
        if inc and alert.ts - inc.end_ts <= self.merge_gap_s:
            inc.end_ts = max(inc.end_ts, alert.ts)
            inc.alerts += 1
            if alert.value is not None and (inc.peak_value is None or alert.value > inc.peak_value):
                inc.peak_value = alert.value
            # An outage reported by the sweep and then "restored" is one incident.
            if alert.kind == AlertKind.OUTAGE and "restored" in alert.detail:
                inc.detail = alert.detail
            return inc, False
        if inc:
            inc.open = False
        inc = Incident(
            alert.meter_id,
            alert.kind,
            alert.ts,
            alert.ts,
            detail=alert.detail,
            peak_value=alert.value,
            detected_ts=alert.detected_ts if alert.detected_ts is not None else alert.ts,
        )
        self._open[key] = inc
        return inc, True

    def close_stale(self, now: int) -> list[Incident]:
        """Close incidents with no new alert for longer than the merge gap."""
        closed = []
        for key, inc in list(self._open.items()):
            if now - inc.end_ts > self.merge_gap_s and inc.kind != AlertKind.OUTAGE:
                inc.open = False
                closed.append(inc)
                del self._open[key]
        return closed

    def resolve(self, meter_id: str, kind: AlertKind) -> Incident | None:
        inc = self._open.pop((meter_id, kind), None)
        if inc:
            inc.open = False
        return inc
