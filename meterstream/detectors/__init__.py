"""Streaming, per-meter anomaly detectors.

Every detector is O(1) memory per meter and O(1) work per reading, so the worker can process a
fleet of meters in a single pass without windows or batch jobs.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Protocol

from meterstream.models import NOMINAL_VOLTAGE, Alert, AlertKind, Reading


class Detector(Protocol):
    def observe(self, reading: Reading) -> list[Alert]: ...


@dataclass
class _Ewma:
    """Exponentially weighted mean and variance (West, 1979)."""

    alpha: float
    mean: float = 0.0
    var: float = 0.0
    n: int = 0

    def update(self, x: float) -> None:
        if self.n == 0:
            self.mean, self.var = x, 0.0
        else:
            diff = x - self.mean
            incr = self.alpha * diff
            self.mean += incr
            self.var = (1 - self.alpha) * (self.var + diff * incr)
        self.n += 1

    @property
    def std(self) -> float:
        return math.sqrt(self.var)


HOURS = 24


@dataclass
class SpikeDetector:
    """Flags consumption far above what this meter normally draws at this time of day.

    Keeps one EWMA of log(power) per meter per hour of day, because 3 kW at 7 pm is normal
    and 3 kW at 3 am is not. Log scale makes the test multiplicative ("8x usual") rather than
    additive, which suits loads that range from 0.1 to 10 kW. Anomalous readings are not fed
    back into the baseline, so a long spike can't teach the detector that spikes are normal.

    Meters with rooftop solar report *net* power, which swings with cloud cover in daylight,
    so their daylight readings are skipped (see README: known limitations).
    """

    z_threshold: float = 4.0
    min_ratio: float = 3.0  # also require power >= 3x the usual level
    min_kw: float = 1.0  # ignore tiny loads where ratios are noisy
    warmup: int = 3  # samples per hour bucket before alerting
    alpha: float = 0.1
    utc_offset_h: float = 0.0
    solar_meters: set[str] = field(default_factory=set)
    daylight_hours: tuple[int, int] = (6, 18)
    _stats: dict[tuple[str, int], _Ewma] = field(default_factory=dict)

    def observe(self, r: Reading) -> list[Alert]:
        hour = int(((r.ts % 86_400) / 3600 + self.utc_offset_h) % HOURS)
        if r.meter_id in self.solar_meters and self.daylight_hours[0] <= hour < self.daylight_hours[1]:
            return []
        key = (r.meter_id, hour)
        stats = self._stats.setdefault(key, _Ewma(self.alpha))
        x = math.log(max(r.power_kw, 0.01))
        alerts: list[Alert] = []
        if stats.n >= self.warmup:
            std = max(stats.std, 0.15)  # floor: never trust a near-zero variance
            z = (x - stats.mean) / std
            usual_kw = math.exp(stats.mean)
            if z > self.z_threshold and r.power_kw >= self.min_kw and r.power_kw >= self.min_ratio * usual_kw:
                alerts.append(
                    Alert(
                        r.meter_id,
                        r.ts,
                        AlertKind.SPIKE,
                        f"{r.power_kw:.2f} kW vs usual {usual_kw:.2f} kW at {hour:02d}h (z={z:.1f})",
                        r.power_kw,
                    )
                )
                return alerts  # don't learn from the anomaly
        stats.update(x)
        return alerts


@dataclass
class TamperDetector:
    """Rule-based tamper checks on the cumulative registers.

    * The import register is monotonic: if it goes down, the meter was reset or wound back.
    * A meter registered without generation should never export energy.
    """

    solar_meters: set[str] = field(default_factory=set)
    export_tolerance_kwh: float = 0.01
    _last: dict[str, tuple[float, float]] = field(default_factory=dict)

    def observe(self, r: Reading) -> list[Alert]:
        alerts: list[Alert] = []
        prev = self._last.get(r.meter_id)
        if prev:
            prev_import, prev_export = prev
            if r.energy_import_kwh + 1e-6 < prev_import:
                drop = prev_import - r.energy_import_kwh
                alerts.append(
                    Alert(
                        r.meter_id, r.ts, AlertKind.REGISTER_ROLLBACK, f"import register fell by {drop:.1f} kWh", drop
                    )
                )
            exported = r.energy_export_kwh - prev_export
            if r.meter_id not in self.solar_meters and exported > self.export_tolerance_kwh:
                alerts.append(
                    Alert(
                        r.meter_id,
                        r.ts,
                        AlertKind.REVERSE_FLOW,
                        f"{exported:.3f} kWh exported by a meter with no generation",
                        exported,
                    )
                )
        self._last[r.meter_id] = (r.energy_import_kwh, r.energy_export_kwh)
        return alerts


@dataclass
class VoltageDetector:
    """Statutory supply limits: nominal ±10% (EN 50160 / IS 12360). Outside = sag or swell."""

    nominal: float = NOMINAL_VOLTAGE
    tolerance: float = 0.10

    def observe(self, r: Reading) -> list[Alert]:
        low, high = self.nominal * (1 - self.tolerance), self.nominal * (1 + self.tolerance)
        if 0 < r.voltage_v < low:
            return [Alert(r.meter_id, r.ts, AlertKind.VOLTAGE_SAG, f"{r.voltage_v:.1f} V < {low:.0f} V", r.voltage_v)]
        if r.voltage_v > high:
            return [
                Alert(r.meter_id, r.ts, AlertKind.VOLTAGE_SWELL, f"{r.voltage_v:.1f} V > {high:.0f} V", r.voltage_v)
            ]
        return []


@dataclass
class OutageDetector:
    """Detects meters that stop reporting.

    `observe` closes an outage when a meter comes back after a gap; `sweep(now)` raises one for
    meters that are silent right now. Gaps are judged from sequence numbers as well as time, so a
    burst of late messages isn't mistaken for an outage.
    """

    interval_s: int = 300
    missed_intervals: int = 3  # silent for 3+ intervals = outage
    _last: dict[str, tuple[int, int]] = field(default_factory=dict)  # meter -> (ts, seq)
    _reported: set[str] = field(default_factory=set)

    def observe(self, r: Reading) -> list[Alert]:
        alerts: list[Alert] = []
        prev = self._last.get(r.meter_id)
        if prev:
            prev_ts, _ = prev
            gap = r.ts - prev_ts
            if gap >= (self.missed_intervals + 1) * self.interval_s:
                minutes = (gap - self.interval_s) / 60
                alerts.append(
                    Alert(
                        r.meter_id,
                        prev_ts + self.interval_s,
                        AlertKind.OUTAGE,
                        f"silent for {minutes:.0f} min, restored",
                        minutes,
                    )
                )
        if not prev or r.ts > prev[0]:
            self._last[r.meter_id] = (r.ts, r.seq)
        self._reported.discard(r.meter_id)
        return alerts

    def sweep(self, now: int) -> list[Alert]:
        """Call periodically: alerts once for each meter that is silent right now."""
        alerts = []
        for meter_id, (last_ts, _) in self._last.items():
            if meter_id in self._reported:
                continue
            if now - last_ts >= (self.missed_intervals + 1) * self.interval_s:
                self._reported.add(meter_id)
                alerts.append(
                    Alert(
                        meter_id,
                        last_ts + self.interval_s,
                        AlertKind.OUTAGE,
                        f"no data for {(now - last_ts) / 60:.0f} min",
                        None,
                    )
                )
        return alerts
