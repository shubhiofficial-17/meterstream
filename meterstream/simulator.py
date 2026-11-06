"""Fleet simulator: realistic household load curves with labelled, injected anomalies.

Each meter gets a household profile (base load, morning/evening peaks, weekend shift, optional
rooftop solar that exports at midday). Anomalies are injected at random with ground-truth labels,
so detector precision and recall can be measured (see eval/evaluate_detectors.py).
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterator
from dataclasses import dataclass, field

from meterstream.models import NOMINAL_VOLTAGE, AlertKind, Reading


@dataclass(frozen=True)
class Label:
    meter_id: str
    kind: AlertKind
    start_ts: int
    end_ts: int


@dataclass
class MeterProfile:
    meter_id: str
    base_kw: float
    peak_kw: float
    has_solar: bool
    solar_kw: float
    phase_shift_h: float  # households don't all wake at the same time


@dataclass
class _MeterState:
    profile: MeterProfile
    import_kwh: float = 0.0
    export_kwh: float = 0.0
    seq: int = 0
    # active anomaly: (kind, remaining intervals, parameter)
    active: tuple[AlertKind, int, float] | None = None


@dataclass
class SimConfig:
    meters: int = 200
    interval_s: int = 300  # 5-minute readings
    start_ts: int = 1_767_225_600  # 2026-01-01T00:00:00Z
    seed: int = 42
    anomaly_rate: float = 0.002  # probability per meter per interval of starting an anomaly
    solar_share: float = 0.25
    duplicate_rate: float = 0.002  # at-least-once delivery: some messages arrive twice
    kinds: tuple[AlertKind, ...] = field(
        default=(
            AlertKind.SPIKE,
            AlertKind.REGISTER_ROLLBACK,
            AlertKind.REVERSE_FLOW,
            AlertKind.OUTAGE,
            AlertKind.VOLTAGE_SAG,
            AlertKind.VOLTAGE_SWELL,
        )
    )


def load_shape(hour: float) -> float:
    """Typical residential daily curve in [0, 1]: overnight trough, morning bump, evening peak."""
    morning = math.exp(-((hour - 7.5) ** 2) / 2.0)
    evening = 1.3 * math.exp(-((hour - 19.5) ** 2) / 4.0)
    return min(1.0, 0.15 + 0.6 * morning + 0.7 * evening)


def solar_shape(hour: float) -> float:
    return max(0.0, math.sin(math.pi * (hour - 6) / 12)) if 6 <= hour <= 18 else 0.0


class FleetSimulator:
    def __init__(self, config: SimConfig | None = None):
        self.cfg = config or SimConfig()
        self.rng = random.Random(self.cfg.seed)
        self.labels: list[Label] = []
        self.meters = [_MeterState(self._profile(i)) for i in range(self.cfg.meters)]
        for m in self.meters:
            m.import_kwh = self.rng.uniform(1_000, 20_000)  # meters are not new

    def _profile(self, i: int) -> MeterProfile:
        r = self.rng
        has_solar = r.random() < self.cfg.solar_share
        return MeterProfile(
            meter_id=f"MTR-{i:06d}",
            base_kw=r.uniform(0.15, 0.5),
            peak_kw=r.uniform(1.5, 4.0),
            has_solar=has_solar,
            solar_kw=r.uniform(2.0, 5.0) if has_solar else 0.0,
            phase_shift_h=r.uniform(-1.0, 1.0),
        )

    def _maybe_start_anomaly(self, m: _MeterState, ts: int) -> None:
        if m.active or self.rng.random() >= self.cfg.anomaly_rate:
            return
        kinds = [k for k in self.cfg.kinds if not (k == AlertKind.REVERSE_FLOW and m.profile.has_solar)]
        if not kinds:
            return
        kind = self.rng.choice(kinds)
        r = self.rng
        duration, param = {
            AlertKind.SPIKE: (r.randint(1, 3), r.uniform(6.0, 12.0)),  # x times normal power
            AlertKind.REGISTER_ROLLBACK: (1, r.uniform(50, 500)),  # kWh wound back
            AlertKind.REVERSE_FLOW: (r.randint(3, 12), r.uniform(0.5, 2.0)),  # kW pushed backwards
            AlertKind.OUTAGE: (r.randint(4, 36), 0.0),  # intervals of silence
            AlertKind.VOLTAGE_SAG: (r.randint(1, 4), r.uniform(0.70, 0.86)),  # x nominal
            AlertKind.VOLTAGE_SWELL: (r.randint(1, 4), r.uniform(1.13, 1.25)),
        }[kind]
        m.active = (kind, duration, param)
        end = ts + (duration - 1) * self.cfg.interval_s
        self.labels.append(Label(m.profile.meter_id, kind, ts, end))

    def _reading(self, m: _MeterState, ts: int) -> Reading | None:
        p, r, dt_h = m.profile, self.rng, self.cfg.interval_s / 3600
        day_s = ts % 86_400
        weekday = (ts // 86_400 + 3) % 7  # 1970-01-01 was a Thursday
        hour = (day_s / 3600 + p.phase_shift_h + (1.0 if weekday >= 5 else 0.0)) % 24
        power = p.base_kw + (p.peak_kw - p.base_kw) * load_shape(hour)
        power *= r.lognormvariate(0, 0.15)  # appliance noise
        solar = p.solar_kw * solar_shape(hour) * r.uniform(0.6, 1.0) if p.has_solar else 0.0
        voltage = NOMINAL_VOLTAGE + r.gauss(0, 3.0)
        export_kw = 0.0

        kind, param = (m.active[0], m.active[2]) if m.active else (None, 0.0)
        if kind == AlertKind.OUTAGE:
            self._tick_anomaly(m)
            return None  # meter is silent
        if kind == AlertKind.SPIKE:
            power *= param
        elif kind == AlertKind.REGISTER_ROLLBACK:
            m.import_kwh = max(0.0, m.import_kwh - param)
        elif kind == AlertKind.REVERSE_FLOW:
            export_kw = param
        elif kind in (AlertKind.VOLTAGE_SAG, AlertKind.VOLTAGE_SWELL):
            voltage = NOMINAL_VOLTAGE * param + r.gauss(0, 2.0)

        net = power - solar
        if net >= 0:
            m.import_kwh += net * dt_h
        else:
            export_kw += -net
        m.export_kwh += export_kw * dt_h
        self._tick_anomaly(m)
        m.seq += 1
        return Reading(
            meter_id=p.meter_id,
            ts=ts,
            seq=m.seq,
            energy_import_kwh=round(m.import_kwh, 3),
            energy_export_kwh=round(m.export_kwh, 3),
            power_kw=round(max(net, 0.0), 3),
            voltage_v=round(voltage, 1),
        )

    @staticmethod
    def _tick_anomaly(m: _MeterState) -> None:
        if m.active:
            kind, remaining, param = m.active
            m.active = (kind, remaining - 1, param) if remaining > 1 else None

    def run(self, intervals: int) -> Iterator[Reading]:
        """Yield readings in time order, with occasional duplicate deliveries."""
        for step in range(intervals):
            ts = self.cfg.start_ts + step * self.cfg.interval_s
            for m in self.meters:
                # Warm-up: no anomalies in the first day so detectors can learn baselines.
                if step * self.cfg.interval_s >= 86_400:
                    self._maybe_start_anomaly(m, ts)
                reading = self._reading(m, ts)
                if reading is None:
                    continue
                yield reading
                if self.rng.random() < self.cfg.duplicate_rate:
                    yield reading

    def now(self, intervals_done: int) -> int:
        return self.cfg.start_ts + intervals_done * self.cfg.interval_s
