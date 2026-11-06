"""Core data types: meter readings, alerts and incidents."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum

NOMINAL_VOLTAGE = 230.0  # IEC 60038 low-voltage nominal (EU/India)


class AlertKind(StrEnum):
    SPIKE = "consumption_spike"
    REGISTER_ROLLBACK = "register_rollback"  # cumulative import register went backwards: tamper / reset
    REVERSE_FLOW = "reverse_flow"  # energy exported by a meter with no generation: likely tamper
    OUTAGE = "outage"  # meter went silent
    VOLTAGE_SAG = "voltage_sag"
    VOLTAGE_SWELL = "voltage_swell"


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


SEVERITY = {
    AlertKind.SPIKE: Severity.WARNING,
    AlertKind.REGISTER_ROLLBACK: Severity.CRITICAL,
    AlertKind.REVERSE_FLOW: Severity.CRITICAL,
    AlertKind.OUTAGE: Severity.WARNING,
    AlertKind.VOLTAGE_SAG: Severity.WARNING,
    AlertKind.VOLTAGE_SWELL: Severity.WARNING,
}


@dataclass(frozen=True)
class Reading:
    meter_id: str
    ts: int  # unix seconds, start of interval
    seq: int  # per-meter message sequence number (detects duplicates and gaps)
    energy_import_kwh: float  # cumulative register
    energy_export_kwh: float  # cumulative register
    power_kw: float  # average active power over the interval
    voltage_v: float

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> Reading:
        """Parse and validate a message (values may arrive as strings from Redis)."""
        r = cls(
            meter_id=str(d["meter_id"]),
            ts=int(d["ts"]),
            seq=int(d["seq"]),
            energy_import_kwh=float(d["energy_import_kwh"]),
            energy_export_kwh=float(d["energy_export_kwh"]),
            power_kw=float(d["power_kw"]),
            voltage_v=float(d["voltage_v"]),
        )
        if not r.meter_id or r.ts <= 0 or r.seq < 0:
            raise ValueError("bad identifiers")
        if r.power_kw < 0 or r.voltage_v < 0 or r.energy_import_kwh < 0 or r.energy_export_kwh < 0:
            raise ValueError("negative measurement")
        if r.voltage_v > 1000 or r.power_kw > 1000:
            raise ValueError("measurement out of physical range")
        return r


@dataclass(frozen=True)
class Alert:
    meter_id: str
    ts: int
    kind: AlertKind
    detail: str = ""
    value: float | None = None
    detected_ts: int | None = None  # when the pipeline raised it (ts may be backdated, e.g. outages)


@dataclass
class Incident:
    """Consecutive alerts of the same kind on the same meter, merged into one event."""

    meter_id: str
    kind: AlertKind
    start_ts: int
    end_ts: int
    alerts: int = 1
    detail: str = ""
    peak_value: float | None = None
    open: bool = True
    id: str = field(default="")
    detected_ts: int | None = None

    def __post_init__(self) -> None:
        if not self.id:
            self.id = f"{self.meter_id}:{self.kind.value}:{self.start_ts}"

    @property
    def severity(self) -> Severity:
        return SEVERITY[self.kind]

    def to_dict(self) -> dict:
        d = asdict(self)
        d["kind"], d["severity"] = self.kind.value, self.severity.value
        return d
